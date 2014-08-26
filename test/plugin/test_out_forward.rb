require 'fluent/test'
require 'helper'

class ForwardOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  TARGET_HOST = '127.0.0.1'
  TARGET_PORT = 13999
  CONFIG = %[
    send_timeout 51
    <server>
      name test
      host #{TARGET_HOST}
      port #{TARGET_PORT}
    </server>
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::OutputTestDriver.new(Fluent::ForwardOutput) do
      attr_reader :responses, :exceptions

      alias :original_send_data :send_data

      def send_data(node, tag, chunk)
        @responses ||= []
        @responses << original_send_data(node, tag, chunk)
      rescue => e
        @exceptions ||= []
        @exceptions << e
        raise e
      end
    end.configure(conf)
  end

  def test_configure
    d = create_driver
    nodes = d.instance.nodes
    assert_equal 51, d.instance.send_timeout
    assert_equal :udp, d.instance.heartbeat_type
    assert_equal 1, nodes.length
    node = nodes.first
    assert_equal "test", node.name
    assert_equal '127.0.0.1', node.host
    assert_equal 13999, node.port
  end

  def test_configure_tcp_heartbeat
    d = create_driver(CONFIG + "\nheartbeat_type tcp")
    assert_equal :tcp, d.instance.heartbeat_type
  end

  def test_phi_failure_detector
    d = create_driver(CONFIG + %[phi_failure_detector false \n phi_threshold 0])
    node = d.instance.nodes.first
    stub(node.failure).phi { raise 'Should not be called' }
    node.tick
    assert_equal node.available, true

    d = create_driver(CONFIG + %[phi_failure_detector true \n phi_threshold 0])
    node = d.instance.nodes.first
    node.tick
    assert_equal node.available, false
  end

  def test_wait_response_timeout_config
    d = create_driver(CONFIG)
    assert_nil d.instance.wait_response_timeout

    d = create_driver(CONFIG + %[wait_response_timeout 2s])
    assert_equal 2, d.instance.wait_response_timeout
  end

  def test_send_data
    input_driver = WrapperDriver.new(WrapperForwardInput, TARGET_HOST, TARGET_PORT)

    d = create_driver(CONFIG + %[flush_interval 1s])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    d.expected_emits_length = records.length
    # TODO: set when d.run ends

    input_driver.start
    d.run do
      records.each do |record|
        d.emit record, time
      end
    end
    input_driver.shutdown

    emits = input_driver.emits
    assert_equal ['test', time, records[0]], emits[0]
    assert_equal ['test', time, records[1]], emits[1]
  end

  def test_send_data_with_option
    input_driver = WrapperDriver.new(WrapperForwardInput, TARGET_HOST, TARGET_PORT)

    d = create_driver(CONFIG + %[
      flush_interval 1s
      wait_response_timeout 1s
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    d.expected_emits_length = records.length
    # TODO: set when d.run ends

    input_driver.start
    d.run do
      records.each do |record|
        d.emit record, time
      end
    end
    input_driver.shutdown

    emits = input_driver.emits
    assert_equal ['test', time, records[0]], emits[0]
    assert_equal ['test', time, records[1]], emits[1]

    assert_equal 1, d.instance.responses.length
    assert d.instance.responses[0].has_key?('ack') # TODO: can assert value?
    assert_nil d.instance.exceptions
  end

  class WrapperDriver
    def initialize(wrapper_klass, *args)
      raise ArgumentError unless wrapper_klass.is_a?(Class)
      @instance = wrapper_klass.new(*args)
      @engine = DummyEngineClass.new
      @instance.class.superclass.const_set(:Engine, @engine) # can not run tests concurrently
    end

    def start
      @instance.start
    end

    def shutdown
      @instance.shutdown
      @instance.class.superclass.class_eval do
        remove_const(:Engine)
      end
    end

    def emits
      all = []
      @engine.emit_streams.each {|tag,events|
        events.each {|time,record|
          all << [tag, time, record]
        }
      }
      all
    end

    class DummyEngineClass
      attr_reader :emit_streams

      def initialize
        @emit_streams = []
      end

      def emit_stream(tag, es)
        @emit_streams << [tag, es.to_a]
      end
    end
  end

  require 'fluent/plugin/in_forward'

  class WrapperForwardInput < Fluent::ForwardInput
    def initialize(host, port)
      @host = host
      @port = port
    end

    def start
      @thread = Thread.new do
        Socket.tcp_server_loop(@host, @port) do |sock, client_addrinfo|
          begin
            handler = WrapperHandler.new(sock, $log, method(:on_message))
            loop do
              raw_data = sock.recv(1024)
              handler.on_read(raw_data)
              break if handler.chunk_counter == 0
            end
          ensure
            sock.close
          end
        end
      end
    end

    def shutdown
      @thread.kill
      @thread.join
    end

    class WrapperHandler < Handler
      attr_reader :chunk_counter # for checking if received data is successfully unpacked

      def initialize(sock, log, on_message)
        @sock = sock
        @log = log
        @chunk_counter = 0
        @on_message = on_message
      end

      def write(data)
        @sock.write data
      rescue => e
        @sock.close
      end

      def close
        @sock.close
      end
    end
  end
end
