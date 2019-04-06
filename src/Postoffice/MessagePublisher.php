<?php

namespace CodeKobold\Postoffice;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * class MessagePublisher.
 *
 * AMQP Topic Exchange Message Publisher for RabbitMQ.
 *
 * @author Ron Metten <code-kobold@keemail.me>
 */
class MessagePublisher
{
    /** @var AMQPStreamConnection */
    private $connection;

    /** @var AMQPChannel */
    private $channel;

    /** @var string */
    private $exchange;

    /** @var string */
    private $queueName;

    /**
     * MessagePublisher CTor.
     *
     * @param string $host
     * @param int    $port
     * @param string $user
     * @param string $password
     * @param string $vHost
     * @param string $exchange
     * @param string $queueName
     */
    public function __construct(string $host, int $port, string $user, string $password, string $vHost, string $exchange, $queueName = '')
    {
        $this->exchange = $exchange;
        $this->queueName = $queueName;

        $this->connection = new AMQPStreamConnection($host, $port, $user, $password, $vHost);
        $this->channel = $this->connection->channel();
        $this->channel->exchange_declare($this->exchange, 'topic', false, true, false);

        if ('' !== $queueName) {
            $this->channel->queue_declare($this->queueName, false, true, false, false);
            $this->channel->queue_bind($this->queueName, $this->exchange);
        }
    }

    /**
     * Send Message.
     *
     * @param string $body
     * @param string $routingKey
     */
    public function sendMessage(string $body, string $routingKey = '#')
    {
        $message = new AMQPMessage(
            $body,
            array('content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT)
        );

        $this->channel->basic_publish($message, $this->exchange, $routingKey);
    }

    /**
     * Terminate Connection.
     */
    public function close()
    {
        $this->channel->close();
        $this->connection->close();
    }
}
