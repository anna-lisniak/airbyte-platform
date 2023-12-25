import { NotificationItem, NotificationSettings } from "core/api/types/AirbyteClient";

import { notificationKeys, NotificationSettingsFormValues } from "./NotificationSettingsForm";

export function formValuesToNotificationSettings(formValues: NotificationSettingsFormValues): NotificationSettings {
  const notificationSettings: NotificationSettings = {};

  notificationKeys.forEach((notificationKey) => {
    const valueFromForm = formValues[notificationKey];
    const notificationItem: NotificationItem = { notificationType: [] };
    if (valueFromForm.customerio) {
      notificationItem.notificationType?.push("customerio");
    }
    if (valueFromForm.slack) {
      notificationItem.notificationType?.push("slack");
    }
    if (valueFromForm.api) {
      notificationItem.notificationType?.push("api");
    }
    if (valueFromForm.slackWebhookLink) {
      notificationItem.slackConfiguration = {
        webhook: valueFromForm.slackWebhookLink,
      };
      notificationItem.apiConfiguration = {
        webhook: valueFromForm.slackWebhookLink,
      };
    }
    notificationSettings[notificationKey] = notificationItem;
  });

  return notificationSettings;
}
