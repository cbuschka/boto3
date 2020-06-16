# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import logging

from botocore.exceptions import MissingParametersError
from botocore.errorfactory import ClientError

logger = logging.getLogger(__name__)


def register_queue_methods(base_classes, **kwargs):
    base_classes.insert(0, QueueResource)


# This class can be used to add any additional methods we want
# onto a queue resource.  Ideally to avoid creating a new
# base class for every method we can just update this
# class instead.  Just be sure to move the bulk of the
# actual method implementation to another class.
class QueueResource(object):
    def __init__(self, *args, **kwargs):
        super(QueueResource, self).__init__(*args, **kwargs)

    def batch_sender(self):
        """Create a batch sender object.

        This method creates a context manager for writing
        objects to Amazon SQS in batch.

        The batch sender will automatically handle buffering and sending items
        in batches.  In addition, the batch sender will also automatically
        handle any unprocessed messages and resend them as needed.  All you need
        to do is call ``send_message`` for any messages you want to add.

        Example usage::

            with queue.batch_sender() as batch:
                for _ in xrange(1000000):
                    batch.send_message(Message={"MessageId":...})
        """
        return BatchSender(self.url, self.meta.client)


class BatchSender(object):
    """Automatically handle batch sends to SQS for a single queue."""

    def __init__(self, queue_url, client, overwrite_by_id=False, flush_amount=10):
        """

        :type queue_url: str
        :param queue_url: The url of the queue.  The class handles
            batch sends to a single queue.

        :type client: ``botocore.client.Client``
        :param client: A botocore client..

        :type overwrite_by_id: bool
        :param overwrite_by_id: Replace messages in buffer
            if id of new message matches. i.e
            ``{"Id": ...}``

        :type flush_amount: int
        :param flush_amount: The number of messages to keep in
            a local buffer before sending a send_message_batch
            request to SQS.
        """
        self._queue_url = queue_url
        self._client = client
        self._messages_buffer = []
        self._overwrite_by_id = overwrite_by_id
        self._flush_amount = min(max(flush_amount, 1), 10)

    def send_message(self, **kwargs):
        """

        Same arguments as to an entry of client.send_message_batch():
        Id, MessageBody, DelaySeconds, MessageAttributes, MessageSystemAttributes,
        MessageDeduplicationId, MessageGroupId
        """
        if not "Id" in kwargs:
            raise MissingParametersError(object=kwargs, object_name="Message", missing="Id")
        self._add_message_and_process(kwargs)

    def _add_message_and_process(self, entry):
        if self._overwrite_by_id:
            self._remove_dup_ids_entry_if_any(entry)
        self._messages_buffer.append(entry)
        self._flush_if_needed()

    def _remove_dup_ids_entry_if_any(self, new_entry):
        for entry in self._messages_buffer:
            if new_entry["Id"] == entry["Id"]:
                self._messages_buffer.remove(entry)
                logger.debug("With overwrite_by_ids enabled, skipping "
                             "message:%s", entry)

    def _flush_if_needed(self):
        if len(self._messages_buffer) >= self._flush_amount:
            self._flush()

    def _flush(self):
        messages_to_send = self._messages_buffer[:self._flush_amount]
        self._messages_buffer = self._messages_buffer[self._flush_amount:]
        response = self._client.send_message_batch(QueueUrl=self._queue_url, Entries=messages_to_send)
        failed_messages = {e["Id"]: e for e in response.get("Failed", [])}
        self._readd_retryable_messages(failed_messages, messages_to_send)
        self._fail_for_finally_failed_messages(failed_messages, messages_to_send)

    def _fail_for_finally_failed_messages(self, failed_messages, messages_to_send):
        fatal_failed_messages = [failed_messages[m["Id"]] for m in messages_to_send if
                                 m["Id"] in failed_messages and failed_messages[m["Id"]][
                                     "SenderFault"] is True]
        if len(fatal_failed_messages) > 0:
            raise ClientError({"Code": fatal_failed_messages[0]["Code"],
                               "Message": fatal_failed_messages[0]["Code"]},
                              "SendMessage")

    def _readd_retryable_messages(self, failed_messages, messages_to_send):
        retryable_failed_messages = [m for m in messages_to_send if
                                     m["Id"] in failed_messages and failed_messages[m["Id"]]["SenderFault"] is False]
        self._messages_buffer.extend(retryable_failed_messages)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        # When we exit, we need to keep flushing whatever's left
        # until there's nothing left in our items buffer.
        while len(self._messages_buffer) > 0:
            self._flush()
