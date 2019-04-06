<?php

namespace CodeKobold\Postoffice;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;

/**
 * class MessageSubscriber.
 *
 * AMQP Topic Exchange Message Subscriber for RabbitMQ.
 *
 * @author Ron Metten <code-kobold@keemail.me>
 */
class MessageSubscriber
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
     * MessageSubscriber CTor.
     *
     * @param string $host
     * @param int    $port
     * @param string $user
     * @param string $password
     * @param string $vHost
     * @param string $exchange
     * @param array  $bindingKeys
     * @param string $queueName
     */
    public function __construct(string $host, int $port, string $user, string $password, string $vHost, string $exchange, array $bindingKeys = ['#'], $queueName = '')
    {
        $this->exchange = $exchange;
        $this->queueName = $queueName;

        $this->connection = new AMQPStreamConnection($host, $port, $user, $password, $vHost);
        $this->channel = $this->connection->channel();
        $this->channel->exchange_declare($this->exchange, 'topic', false, true, false);

        if ('' !== $queueName) {
            $this->channel->queue_declare($this->queueName, false, true, false, false);
            $this->channel->queue_bind($this->queueName, $this->exchange);
        } else {
            list($this->queueName, ,) = $this->channel->queue_declare('', false, true, false, false);
        }

        foreach ($bindingKeys as $bindingKey) {
            $this->channel->queue_bind($this->queueName, $this->exchange, $bindingKey);
        }
    }

    /**
     * @return AMQPChannel
     */
    public function getChannel(): AMQPChannel
    {
        return $this->channel;
    }

    /**
     * @return string
     */
    public function getQueueName(): string
    {
        return $this->queueName;
    }

    /**
     * Terminate Connection.
     */
    public function close()
    {
        $this->channel->close();
        $this->connection->close();
    }

    /**
     * @param callable $callback
     * @param string   $consumerTag
     * @param bool     $noLocal
     * @param bool     $noAck
     * @param bool     $exclusive
     * @param bool     $noWait
     *
     * @throws \ErrorException
     */
    public function consume(callable $callback, string $consumerTag = '', bool $noLocal = false, bool $noAck = true, $exclusive = false, bool $noWait= false)
    {
        $this->channel->basic_consume(
            $this->getQueueName(),
            $consumerTag,
            $noLocal,
            $noAck,
            $exclusive,
            $noWait,
            $callback
        );

        while (count($this->channel->callbacks)) {
            $this->channel->wait();
        }
    }
}
