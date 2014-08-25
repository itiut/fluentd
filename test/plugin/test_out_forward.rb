require 'fluent/test'
require 'helper'

class ForwardOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup

    @dummy_forward_input_thread = Thread.new do
      serv = TCPServer.new('127.0.0.1', 13999)
      begin
        loop do
          Thread.new(serv.accept) do |sock|
            dummy_input_forward = DummyForwardInput.new
            dummy_handler = DummyForwardInput::DummyHandler.new(sock, dummy_input_forward.method(:on_message))
            loop do
              raw_data = sock.recv(1024)
              dummy_handler.on_read(raw_data)
              break if dummy_handler.chunk_counter == 0
            end
          end
        end
      ensure
        serv.close
      end
    end

  end

  def teardown
    @dummy_forward_input_thread.kill
    @dummy_forward_input_thread.join
  end

  CONFIG = %[
    send_timeout 51
    <server>
      name test
      host 127.0.0.1
      port 13999
    </server>
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::OutputTestDriver.new(Fluent::ForwardOutput) do
      # def write(chunk)
      #   chunk.read
      # end
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
    d = create_driver(CONFIG + %[flush_interval 1s])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    d.expected_emits_length = records.length

    d.run do
      records.each do |record|
        d.emit record, time
      end
    end

    emits = d.emits
    assert_equal ['test', time, records[0]], emits[0]
    assert_equal ['test', time, records[1]], emits[1]
  end


  require 'fluent/plugin/in_forward'

  class DummyForwardInput < Fluent::ForwardInput
    def initialize
      # do nothing
    end

    class DummyHandler < Handler
      attr_reader :chunk_counter # for checking if received data is successfully unpacked

      def initialize(sock, on_message)
        @sock = sock
        # @log =
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
