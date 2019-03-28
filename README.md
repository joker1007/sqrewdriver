# sqrewdriver

This gem extend Amazon SQS client. (parallelize and aggregation).

## Features

- Message buffering and Use SendMessageBatch implicitly
- Sending message on thread pool by [concurrent-ruby](https://github.com/ruby-concurrency/concurrent-ruby)
- Message Aggregation into serialized array
- Serialize message implicitly (default: JSON format)

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'sqrewdriver'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install sqrewdriver

## Usage

```ruby
client = Sqrewdriver::Client.new(queue_url: "https://sqs.ap-northeast-1.amazonaws.com/dummy/dummy")

# or

sqs = Aws::SQS::Client.new(log_level: :debug)
client = Sqrewdriver::Client.new(
  queue_url: "https://sqs.ap-northeast-1.amazonaws.com/dummy/dummy",
  client: sqs
)

# If use message aggregation

client = Sqrewdriver::Client.new(
  queue_url: "https://sqs.ap-northeast-1.amazonaws.com/dummy/dummy",
  aggregate_messages_per: 100
) # one SQS message is a serialized array having 100 items.

payload_data.each do |d|
  client.send_message_buffered(message_body: d)
end

client.flush # Don't forget to call flush finally

# If any exception occurs when client is flushing messages,
# client raises `Sqrewdriver::SendMessageErrors`
```

### Change serializer

Serializer needs `dump(value)` method.

```ruby
client = Sqrewdriver::Client.new(
  queue_url: "https://sqs.ap-northeast-1.amazonaws.com/dummy/dummy",
  serializer: YAML
)
```

If you want to pass options to serializer, Please implement wrapping class.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/joker1007/sqrewdriver.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
