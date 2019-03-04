RSpec.describe SQS::Enhanced::Client, aggregate_failures: true do
  let(:sqs) { Aws::SQS::Client.new(stub_responses: true) }
  let(:client) { SQS::Enhanced::Client.new(client: sqs) }
  let(:queue_url) { "http://dummy.example.com" }

  describe "#send_message_buffered" do
    it "add message to buffer" do
      client.send_message_buffered(queue_url: queue_url, message_body: "body")
      buffer = client.instance_variable_get(:@send_message_buffer)
      current_buffer_size = client.instance_variable_get(:@current_buffer_size)

      expect(buffer[queue_url][0]).to eq({message_body: "body"})
      expect(current_buffer_size[queue_url].value).to eq(4)
    end

    it "send message to SQS when need_flush (by message count)" do
      sent_queue_url = nil
      entries = nil
      sqs.stub_responses(:send_message_batch, -> (ctx) {
        sent_queue_url = ctx.params[:queue_url]
        entries = ctx.params[:entries]
        sqs.stub_data(:send_message_batch)
      })
      10.times do
        client.send_message_buffered(queue_url: queue_url, message_body: "body")
      end
      Concurrent::Promises.zip_futures(*client.instance_variable_get(:@waiting_futures)).wait!
      buffer = client.instance_variable_get(:@send_message_buffer)
      current_buffer_size = client.instance_variable_get(:@current_buffer_size)

      expect(buffer[queue_url]).to be_empty
      expect(current_buffer_size[queue_url].value).to eq(0)
      expect(sent_queue_url).to eq(queue_url)
      expect(entries).to eq([
        {message_body: "body", id: "0"},
        {message_body: "body", id: "1"},
        {message_body: "body", id: "2"},
        {message_body: "body", id: "3"},
        {message_body: "body", id: "4"},
        {message_body: "body", id: "5"},
        {message_body: "body", id: "6"},
        {message_body: "body", id: "7"},
        {message_body: "body", id: "8"},
        {message_body: "body", id: "9"},
      ])
    end

    it "send message to SQS when need_flush (by payload size)" do
      sent_queue_url = nil
      entries = nil
      sqs.stub_responses(:send_message_batch, -> (ctx) {
        sent_queue_url = ctx.params[:queue_url]
        entries = ctx.params[:entries]
        sqs.stub_data(:send_message_batch)
      })
      2.times do
        client.send_message_buffered(queue_url: queue_url, message_body: "a" * (128 * 1024))
      end
      Concurrent::Promises.zip_futures(*client.instance_variable_get(:@waiting_futures)).wait!
      buffer = client.instance_variable_get(:@send_message_buffer)
      current_buffer_size = client.instance_variable_get(:@current_buffer_size)

      expect(buffer[queue_url]).to be_empty
      expect(current_buffer_size[queue_url].value).to eq(0)
      expect(sent_queue_url).to eq(queue_url)
      expect(entries.size).to eq(2)
    end
  end
end
