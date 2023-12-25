/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorType;
import io.airbyte.config.SlackNotificationConfiguration;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: redact description Notification client that uses Slack API for Incoming Webhook to send
 * messages.
 *
 * This class also reads a resource YAML file that defines the template message to send.
 *
 * It is stored as a YAML so that we can easily change the structure of the JSON data expected by
 * the API that we are posting to (and we can write multi-line strings more easily).
 *
 * For example, slack API expects some text message in the { "text" : "Hello World" } field...
 */
public class ApiNotificationClient extends NotificationClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApiNotificationClient.class);
  private static final String CLIENT_TYPE = "api";
  private static final String SOURCE_CONNECTOR = "sourceConnector";
  private static final String DESTINATION_CONNECTOR = "destinationConnector";
  private static final String JOB_DESCRIPTION = "jobDescription";
  private static final String STATUS = "status";
  private static final String CONNECTION_ID = "connectionId";

  private final SlackNotificationConfiguration config;

  public ApiNotificationClient(final SlackNotificationConfiguration apiNotificationConfiguration) {
    this.config = apiNotificationConfiguration;
  }

  @Override
  public boolean notifyJobFailure(final String receiverEmail,
                                  final String sourceConnector,
                                  final String destinationConnector,
                                  final String connectionName,
                                  final String jobDescription,
                                  final String logUrl,
                                  final Long jobId)
      throws IOException, InterruptedException {
    final ImmutableMap<String, String> body = new ImmutableMap.Builder<String, String>()
        .put("connectionName", connectionName)
        .put(SOURCE_CONNECTOR, sourceConnector)
        .put(DESTINATION_CONNECTOR, destinationConnector)
        .put(JOB_DESCRIPTION, jobDescription)
        .put("logUrl", logUrl)
        .put("jobId", String.valueOf(jobId))
        .put(STATUS, "failed")
        .build();
    final String bodyJSON = Jsons.serialize(body);

    return notifyFailure(bodyJSON);
  }

  @Override
  public boolean notifyJobSuccess(final String receiverEmail,
                                  final String sourceConnector,
                                  final String destinationConnector,
                                  final String connectionName,
                                  final String jobDescription,
                                  final String logUrl,
                                  final Long jobId)
      throws IOException, InterruptedException {
    LOGGER.info("notifyJobSuccess");
    final ImmutableMap<String, String> body = new ImmutableMap.Builder<String, String>()
        .put("connectionName", connectionName)
        .put(SOURCE_CONNECTOR, sourceConnector)
        .put(DESTINATION_CONNECTOR, destinationConnector)
        .put(JOB_DESCRIPTION, jobDescription)
        .put("logUrl", logUrl)
        .put("jobId", String.valueOf(jobId))
        .put(STATUS, "success")
        .build();
    final String bodyJSON = Jsons.serialize(body);
    LOGGER.info("notifyJobSuccess -> bodyJSON: " + bodyJSON);

    return notifySuccess(bodyJSON);
  }

  @Override
  public boolean notifyConnectionDisabled(final String receiverEmail,
                                          final String sourceConnector,
                                          final String destinationConnector,
                                          final String jobDescription,
                                          final UUID workspaceId,
                                          final UUID connectionId)
      throws IOException, InterruptedException {
    final ImmutableMap<String, String> body = new ImmutableMap.Builder<String, String>()
        // .put("connectionName", connectionName)
        .put("receiverEmail", receiverEmail)
        .put(SOURCE_CONNECTOR, sourceConnector)
        .put(DESTINATION_CONNECTOR, destinationConnector)
        .put(JOB_DESCRIPTION, jobDescription)
        // .put("logUrl", logUrl)
        .put("workspaceId", String.valueOf(workspaceId))
        .put(CONNECTION_ID, String.valueOf(connectionId))
        // .put("jobId", String.valueOf(jobId))
        .put(STATUS, "disabled")
        .build();
    final String bodyJSON = Jsons.serialize(body);

    final String webhookUrl = config.getWebhook();
    if (!Strings.isEmpty(webhookUrl)) {
      return notify(bodyJSON);
    }
    return false;
  }

  @Override
  public boolean notifyConnectionDisableWarning(final String receiverEmail,
                                                final String sourceConnector,
                                                final String destinationConnector,
                                                final String jobDescription,
                                                final UUID workspaceId,
                                                final UUID connectionId)
      throws IOException, InterruptedException {
    final ImmutableMap<String, String> body = new ImmutableMap.Builder<String, String>()
        // .put("connectionName", connectionName)
        .put("receiverEmail", receiverEmail)
        .put(SOURCE_CONNECTOR, sourceConnector)
        .put(DESTINATION_CONNECTOR, destinationConnector)
        .put(JOB_DESCRIPTION, jobDescription)
        // .put("logUrl", logUrl)
        .put("workspaceId", workspaceId.toString())
        .put(CONNECTION_ID, connectionId.toString())
        // .put("jobId", String.valueOf(jobId))
        .put(STATUS, "disable_warning")
        .build();
    final String bodyJSON = Jsons.serialize(body);

    final String webhookUrl = config.getWebhook();
    if (!Strings.isEmpty(webhookUrl)) {
      return notify(bodyJSON);
    }
    return false;
  }

  @Override
  public boolean notifyBreakingChangeWarning(final List<String> receiverEmails,
                                             final String connectorName,
                                             final ActorType actorType,
                                             final ActorDefinitionBreakingChange breakingChange)
      throws IOException, InterruptedException {
    // TODO for api
    // Unsupported for now since we can't reliably send bulk Slack notifications
    throw new UnsupportedOperationException("Slack notification is not supported for breaking change warning");
  }

  @Override
  public boolean notifyBreakingChangeSyncsDisabled(final List<String> receiverEmails,
                                                   final String connectorName,
                                                   final ActorType actorType,
                                                   final ActorDefinitionBreakingChange breakingChange)
      throws IOException, InterruptedException {
    // TODO for api
    // Unsupported for now since we can't reliably send bulk Slack notifications
    throw new UnsupportedOperationException("Slack notification is not supported for breaking change syncs disabled notification");
  }

  @Override
  public boolean notifySchemaChange(final UUID connectionId, final boolean isBreaking, final String url)
      throws IOException, InterruptedException {
    final ImmutableMap<String, String> body = new ImmutableMap.Builder<String, String>()
        .put(CONNECTION_ID, connectionId.toString())
        .put("isBreaking", String.valueOf(isBreaking))
        .put("url", url)
        .put(STATUS, "schema_change")
        .build();
    final String bodyJSON = Jsons.serialize(body);

    final String webhookUrl = config.getWebhook();

    if (!Strings.isEmpty(webhookUrl)) {
      return notify(bodyJSON);
    }
    return false;
  }

  @Override
  public boolean notifySchemaPropagated(final UUID connectionId,
                                        final String sourceName,
                                        final List<String> changes,
                                        final String url,
                                        final List<String> recipients,
                                        boolean isBreaking)
      throws IOException, InterruptedException {
    final String summary = String.join(",", changes);

    final ImmutableMap<String, String> body = new ImmutableMap.Builder<String, String>()
        .put(CONNECTION_ID, connectionId.toString())
        .put("isBreaking", String.valueOf(isBreaking))
        .put("sourceName", sourceName)
        .put("summary", summary)
        .put("url", url)
        .put("recipients", String.join(",", recipients))
        .put(STATUS, "schema_propagation")
        .build();
    final String bodyJSON = Jsons.serialize(body);

    final String webhookUrl = config.getWebhook();
    if (!Strings.isEmpty(webhookUrl)) {
      return notify(bodyJSON);
    }
    return false;
  }

  private boolean notify(final String bodyJSON) throws IOException, InterruptedException {
    LOGGER.info("notify");
    final HttpClient httpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .build();
    LOGGER.info("notify 1");

    final HttpRequest request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers.ofString(bodyJSON))
        .uri(URI.create(config.getWebhook()))
        .header("Content-Type", "application/json")
        .build();
    LOGGER.info("notify 2");

    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    LOGGER.info("api statusCode: " + response.statusCode());
    LOGGER.info("api isSuccessfulHttpResponse: " + isSuccessfulHttpResponse(response.statusCode()));
    if (isSuccessfulHttpResponse(response.statusCode())) {
      LOGGER.info("Successful notification ({}): {}", response.statusCode(), response.body());
      return true;
    } else {
      final String errorMessage = String.format("Failed to deliver notification (%s): %s", response.statusCode(), response.body());
      throw new IOException(errorMessage);
    }
  }

  @Override
  public boolean notifySuccess(final String bodyJSON) throws IOException, InterruptedException {
    LOGGER.info("notifySuccess");
    final String webhookUrl = config.getWebhook();
    LOGGER.info("notifySuccess -> webhookUrl" + webhookUrl);
    LOGGER.info("Strings.isEmpty(webhookUrl) : " + Strings.isEmpty(webhookUrl));
    if (!Strings.isEmpty(webhookUrl)) {
      LOGGER.info("notifySuccess -> if");
      return notify(bodyJSON);
    }
    LOGGER.info("notifySuccess -> end return");
    return false;
  }

  @Override
  public boolean notifyFailure(final String bodyJSON) throws IOException, InterruptedException {
    final String webhookUrl = config.getWebhook();
    if (!Strings.isEmpty(webhookUrl)) {
      return notify(bodyJSON);
    }
    return false;
  }

  @Override
  public String getNotificationClientType() {
    return CLIENT_TYPE;
  }

  /**
   * Used when user tries to test the notification webhook settings on UI.
   */
  @Override
  public boolean notifyTest(String message) throws IOException, InterruptedException {
    final String webhookUrl = config.getWebhook();
    LOGGER.info("webhookUrl: " + webhookUrl);
    LOGGER.info("its from api !!!!!!!");

    final ImmutableMap<String, String> body = new ImmutableMap.Builder<String, String>()
        .put("webhookUrl", webhookUrl)
        .put("status", "test")
        .build();
    final String bodyJSON = Jsons.serialize(body);

    if (!Strings.isEmpty(webhookUrl)) {
      return notify(bodyJSON);
    }
    return false;
  }

  /**
   * Use an integer division to check successful HTTP status codes (i.e., those from 200-299), not
   * just 200. https://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
   */
  private static boolean isSuccessfulHttpResponse(final int httpStatusCode) {
    return httpStatusCode / 100 == 2;
  }

}
