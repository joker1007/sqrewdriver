RSpec.describe Sqrewdriver::Client, aggregate_failures: true do
  let(:sqs) { Aws::SQS::Client.new(stub_responses: true) }
  let(:queue_url) { "http://dummy.example.com" }
  let(:client) { Sqrewdriver::Client.new(queue_url: queue_url, client: sqs) }

  describe "#send_message_buffered" do
    it "add message to buffer" do
      client.send_message_buffered(message_body: {foo: "body"})
      buffer = client.instance_variable_get(:@message_buffer)

      expect(buffer.shift(true)).to eq({message_body: {foo: "body"}})
    end

    it "send message to SQS when need_flush (by message count)" do
      sent_queue_url = nil
      entries = nil
      sqs.stub_responses(:send_message_batch, -> (ctx) {
        sent_queue_url = ctx.params[:queue_url]
        entries = ctx.params[:entries]
        sqs.stub_data(:send_message_batch)
      })
      12.times do
        client.send_message_buffered(message_body: {foo: "body"})
      end
      Concurrent::Promises.zip_futures(*client.instance_variable_get(:@waiting_futures)).wait!
      buffer = client.instance_variable_get(:@message_buffer)

      expect(buffer.size).to eq(2)
      expect(sent_queue_url).to eq(queue_url)
      body = JSON.generate({foo: "body"})
      expect(entries).to eq([
        {message_body: body, id: "0"},
        {message_body: body, id: "1"},
        {message_body: body, id: "2"},
        {message_body: body, id: "3"},
        {message_body: body, id: "4"},
        {message_body: body, id: "5"},
        {message_body: body, id: "6"},
        {message_body: body, id: "7"},
        {message_body: body, id: "8"},
        {message_body: body, id: "9"},
      ])

      client.flush
      expect(buffer).to be_empty
      expect(entries).to eq([
        {message_body: body, id: "0"},
        {message_body: body, id: "1"},
      ])
    end

    it "handles errors" do
      m = Mutex.new
      entries = []
      sqs.stub_responses(:send_message_batch, -> (ctx) {
        first = ctx.params[:entries].first
        sent_entries = ctx.params[:entries].reject { |e| e[:id].to_i == 0 }
        m.synchronize do
          entries.concat(sent_entries)
        end
        sqs.stub_data(:send_message_batch, failed: [{id: first[:id], message: "failed"}])
      })
      12.times do
        client.send_message_buffered(message_body: {foo: "body"})
      end
      buffer = client.instance_variable_get(:@message_buffer)

      expect { client.wait_flushing }.to raise_error(Sqrewdriver::SendMessageErrors)
      expect(buffer.size).to eq(2)
      body = JSON.generate({foo: "body"})
      expect(entries).to eq([
        {message_body: body, id: "1"},
        {message_body: body, id: "2"},
        {message_body: body, id: "3"},
        {message_body: body, id: "4"},
        {message_body: body, id: "5"},
        {message_body: body, id: "6"},
        {message_body: body, id: "7"},
        {message_body: body, id: "8"},
        {message_body: body, id: "9"},
      ])

      entries.clear

      expect { client.flush }.to raise_error(Sqrewdriver::SendMessageErrors)

      expect(buffer).to be_empty
      expect(entries).to eq([
        {message_body: body, id: "1"},
      ])
    end

    it "can avoid race condition on sending buffer (brute force test)" do
      num = 0
      expect {
        begin
          100000.times { |i| num = i
            client.send_message_buffered(message_body: {foo: "body"})
          }
        rescue => e
          puts "Failed with #{e} at ##{num}th iteration"
          raise
        end
      }.not_to raise_error
    end

    context "with buffer_size" do
      let(:client) { Sqrewdriver::Client.new(queue_url: queue_url, client: sqs, buffer_size: 5) }
      it "add message to buffer" do
        client.send_message_buffered(message_body: {foo: "body"})
        buffer = client.instance_variable_get(:@message_buffer)

        expect(buffer.shift(true)).to eq({message_body: {foo: "body"}})
      end

      it "send message to SQS when need_flush (by message count)" do
        sqs.stub_responses(:send_message_batch, -> (ctx) {
          sqs.stub_data(:send_message_batch)
        })
        12.times do
          client.send_message_buffered(message_body: {foo: "body"})
        end
        Concurrent::Promises.zip_futures(*client.instance_variable_get(:@waiting_futures)).wait!
        buffer = client.instance_variable_get(:@message_buffer)

        expect(buffer.size).to eq(2)
        client.flush

        expect(buffer).to be_empty
      end

      it "can avoid race condition on sending buffer (brute force test)" do
        num = 0
        expect {
          begin
            100000.times { |i|
              num = i
              client.send_message_buffered(message_body: {foo: "body"})
            }
          rescue => e
            puts "Failed with #{e} at ##{num}th iteration"
            raise
          end
        }.not_to raise_error
      end

      context "multi thread" do
        it "can send all messages" do
          m = Mutex.new
          entries = []
          sqs.stub_responses(:send_message_batch, -> (ctx) {
            m.synchronize do
              entries.concat(ctx.params[:entries])
            end
            sqs.stub_data(:send_message_batch)
          })
          pool = Concurrent::FixedThreadPool.new(32)
          100.times do
            pool.post do
              15.times do
                client.send_message_buffered(message_body: "body")
              end
            end
          end

          5.times do
            pool.post do
              client.flush_async
            end
          end

          pool.shutdown && pool.wait_for_termination
          client.flush

          buffer = client.instance_variable_get(:@message_buffer)

          expect(buffer).to be_empty
          expect(entries.length).to eq(100 * 15)
        end
      end
    end

    context "multi thread" do
      it "can send all messages" do
        m = Mutex.new
        entries = []
        sqs.stub_responses(:send_message_batch, -> (ctx) {
          m.synchronize do
            entries.concat(ctx.params[:entries])
          end
          sqs.stub_data(:send_message_batch)
        })
        pool = Concurrent::FixedThreadPool.new(32)
        100.times do
          pool.post do
            15.times do
              client.send_message_buffered(message_body: "body")
            end
          end
        end

        5.times do
          pool.post do
            client.flush_async
          end
        end

        pool.shutdown && pool.wait_for_termination
        client.flush

        buffer = client.instance_variable_get(:@message_buffer)

        expect(buffer).to be_empty
        expect(entries.length).to eq(100 * 15)
      end
    end

    context "aggregate_messages_per is set" do
      let(:client) { Sqrewdriver::Client.new(queue_url: queue_url, client: sqs, aggregate_messages_per: 10) }

      it "send message to SQS when need_flush (by aggregate_messages_per)" do
        entries = nil
        sqs.stub_responses(:send_message_batch, -> (ctx) {
          entries = ctx.params[:entries]
          sqs.stub_data(:send_message_batch)
        })
        99.times do
          client.send_message_buffered(message_body: {foo: "body"})
        end
        Concurrent::Promises.zip_futures(*client.instance_variable_get(:@waiting_futures)).wait!
        buffer = client.instance_variable_get(:@message_buffer)
        expect(buffer.size).to eq(99)

        client.send_message_buffered(message_body: {foo: "body"})
        Concurrent::Promises.zip_futures(*client.instance_variable_get(:@waiting_futures)).wait!
        buffer = client.instance_variable_get(:@message_buffer)
        expect(buffer).to be_empty

        body = JSON.generate(10.times.map { {foo: "body"} })
        expect(entries).to eq([
          {message_body: body, id: "0"},
          {message_body: body, id: "1"},
          {message_body: body, id: "2"},
          {message_body: body, id: "3"},
          {message_body: body, id: "4"},
          {message_body: body, id: "5"},
          {message_body: body, id: "6"},
          {message_body: body, id: "7"},
          {message_body: body, id: "8"},
          {message_body: body, id: "9"},
        ])
      end

      context "multi thread" do
        it "can send all messages" do
          entries = []
          sqs.stub_responses(:send_message_batch, -> (ctx) {
            entries.concat(ctx.params[:entries])
            sqs.stub_data(:send_message_batch)
          })
          pool = Concurrent::FixedThreadPool.new(32)
          100.times do
            pool.post do
              15.times do
                client.send_message_buffered(message_body: "body")
              end
            end

            5.times do
              pool.post do
                client.flush_async
              end
            end
          end
          pool.shutdown && pool.wait_for_termination
          client.flush

          buffer = client.instance_variable_get(:@message_buffer)

          expect(buffer).to be_empty
          expect(entries.flat_map { |m| Oj.load(m[:message_body]) }.size).to eq(100 * 15)
        end
      end
    end
  end
end
