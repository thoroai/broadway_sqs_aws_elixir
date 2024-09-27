# BroadwaySQS

A AWS SQS connector for [Broadway](https://github.com/dashbitco/broadway).

This is substantially a copy of [https://hexdocs.pm/broadway_sqs](https://hexdocs.pm/broadway_sqs),
but adapted to the [https://hex.pm/packages/aws](https://hex.pm/packages/aws) AWS client.

An upstream PR was offered to [https://hexdocs.pm/broadway_sqs](https://hexdocs.pm/broadway_sqs),
but was rejected due to the desire to support only one client. Rather than use a fork of that
repository, we decided to make a new repository so that we can independently tag releases.

## Installation

Add `:broadway_sqs` to the list of dependencies in `mix.exs` along with the HTTP
client of your choice (defaults to `:hackney`):

```elixir
def deps do
  [
    {:broadway_sqs, github: "thoroai/broadway_sqs_aws_elixir", ref: "0.1.0"}
  ]
end
```

## Usage

Configure Broadway with one or more producers using `BroadwaySQS.Producer`:

```elixir
Broadway.start_link(MyBroadway,
  name: MyBroadway,
  producer: [
    module: {BroadwaySQS.Producer,
      queue_url: "https://sqs.amazonaws.com/1234567890/queue"
    }
  ]
)
```

## License

Copyright 2019 Plataformatec\
Copyright 2020 Dashbit

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
