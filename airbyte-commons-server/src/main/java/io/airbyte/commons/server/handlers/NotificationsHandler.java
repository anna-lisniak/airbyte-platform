/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.converters.NotificationSettingsConverter.toConfig;

import io.airbyte.api.model.generated.NotificationRead;
import io.airbyte.api.model.generated.NotificationRead.StatusEnum;
import io.airbyte.api.model.generated.NotificationTrigger;
import io.airbyte.api.model.generated.NotificationType;
import io.airbyte.api.model.generated.SlackNotificationConfiguration;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.notification.ApiNotificationClient;
import io.airbyte.notification.NotificationClient;
import io.airbyte.notification.SlackNotificationClient;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler logic for notificationsApiController.
 */
@Singleton
public class NotificationsHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(AttemptHandler.class);

  private static final Map<NotificationTrigger, String> NOTIFICATION_TRIGGER_TEST_MESSAGE = Map.of(
      NotificationTrigger.SYNC_SUCCESS, "Hello World! This is a test from Airbyte to try slack notification settings for sync successes.",
      NotificationTrigger.SYNC_FAILURE, "Hello World! This is a test from Airbyte to try slack notification settings for sync failures.",
      NotificationTrigger.CONNECTION_UPDATE,
      "Hello World! This is a test from Airbyte to try slack notification settings for connection update warning.",
      NotificationTrigger.SYNC_DISABLED, "Hello World! This is a test from Airbyte to try slack notification settings for sync disabled.",
      NotificationTrigger.SYNC_DISABLED_WARNING,
      "Hello World! This is a test from Airbyte to try slack notification settings for your sync is about to be disabled.",
      NotificationTrigger.CONNECTION_UPDATE_ACTION_REQUIRED,
      "Hello World! This is a test from Airbyte to try slack notification settings about your connection has been updated and action is required.");

  /**
   * Send a test notification message to the provided webhook.
   */
  public NotificationRead tryNotification(final SlackNotificationConfiguration slackNotificationConfiguration,
                                          final NotificationTrigger notificationTrigger,
                                          final NotificationType notificationType) {
    LOGGER.info("hello world");
    final String message = NOTIFICATION_TRIGGER_TEST_MESSAGE.get(notificationTrigger);
    LOGGER.info("message: " + message);
    LOGGER.info("notificationType: " + notificationType);

    // Try notification for webhook only.
    // TODO(Xiaohan): SlackNotificationClient should be micronauted so we can mock this object and test
    // this function.
    NotificationClient notificationClient;
    // Notification.NotificationType notificationType = Notification.NotificationType.API;

    if (NotificationType.SLACK.equals(notificationType)) {
      notificationClient = new SlackNotificationClient(toConfig(slackNotificationConfiguration));
    } else if (NotificationType.API.equals(notificationType)) {
      notificationClient =
          new ApiNotificationClient(new io.airbyte.config.SlackNotificationConfiguration().withWebhook(slackNotificationConfiguration.getWebhook()));
    } else {
      return new NotificationRead().status(StatusEnum.FAILED);
    }

    boolean isNotificationSent;

    try {
      isNotificationSent = notificationClient.notifyTest(message);
    } catch (final IllegalArgumentException e) {
      throw new IdNotFoundKnownException(e.getMessage(), notificationTrigger.name(), e);
    } catch (final IOException | InterruptedException e) {
      return new NotificationRead().status(StatusEnum.FAILED).message(e.getMessage());
    }
    LOGGER.info("isNotificationSent: " + isNotificationSent);
    if (isNotificationSent) {
      return new NotificationRead().status(StatusEnum.SUCCEEDED);
    } else {
      return new NotificationRead().status(StatusEnum.FAILED);
    }

  }

}
