using RabbitMQ.Client;
using System.Text;

ConnectionFactory factory = new()
{
    Uri = new Uri("amqp://guest:guest@localhost:5672"),
    ClientProvidedName = "Rabbit Sender App"
};

await using IConnection connection = await factory.CreateConnectionAsync();
await using IChannel channel = await connection.CreateChannelAsync();

string exchangeName = "DemoExchange";
string routingKey = "demo-routing-key";
string queueName = "DemoQueue";

await channel.ExchangeDeclareAsync(
    exchange: exchangeName,
    type: ExchangeType.Direct);

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

await channel.QueueBindAsync(
    queue: queueName,
    exchange: exchangeName,
    routingKey: routingKey);

for (int i = 0; i < 60; i++)
{
    Console.WriteLine($"Sending Message {i}");

    byte[] messageBodyBytes =
        Encoding.UTF8.GetBytes($"Message #{i}");

    await channel.BasicPublishAsync(
        exchange: exchangeName,
        routingKey: routingKey,
        body: messageBodyBytes);

    await Task.Delay(1000);
}