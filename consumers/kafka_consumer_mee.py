"""
kafka_consumer_case.py

Consume messages from a Kafka topic and process them.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> int:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Define a function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a single message.

    For now, this function simply logs the message.
    You can extend it to perform other tasks, like counting words
    or storing data in a database.

    Args:
        message (str): The message to process.
    """
    logger.info(f"Processing message: {message}")

    # Check if the message is "Let's go check out the beach!"
    if message == "Let's go check out the beach!":
        logger.info("ALERT: Special message received: 'Let's go check out the beach!'")
        # Add any other alerting mechanism you want here, like sending an email or triggering a sound.
        # For example, you could raise a custom exception or call another function to send an alert.
        alert_beach_message()

def alert_beach_message() -> None:
    """
    A function to handle special alert actions when the message is "Let's go check out the beach!".
    """
    logger.warning("ALERT: It's time to check out the beach!")
    # Here you can add more alerting functionality (like sending a notification, or an email)
    # For example, you could integrate with an SMS/Email service to send notifications.



#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

     # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
