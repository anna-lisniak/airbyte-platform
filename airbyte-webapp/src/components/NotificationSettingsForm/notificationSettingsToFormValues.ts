import { NotificationItem, NotificationSettings } from "core/request/AirbyteClient";

import { notificationKeys, NotificationSettingsFormValues, NotificationType } from "./NotificationSettingsForm";

export function notificationSettingsToFormValues(
  notificationSettings?: NotificationSettings
): NotificationSettingsFormValues {
  const formValues: NotificationSettingsFormValues = (
    Object.entries(notificationSettings ?? {}) as Array<[keyof NotificationSettings, NotificationItem]>
  ).reduce((acc, [key, value]) => {
    acc[key] = {
      slack: value?.notificationType?.includes("slack") ?? false,
      customerio: value?.notificationType?.includes("customerio") ?? false,
      slackWebhookLink: value?.slackConfiguration?.webhook ?? "",
      type: NotificationType.SLACK,
    };
    return acc;
  }, {} as NotificationSettingsFormValues);

  notificationKeys.forEach((key) => {
    if (!formValues[key]) {
      formValues[key] = {
        slack: false,
        customerio: false,
        slackWebhookLink: "",
        type: NotificationType.SLACK,
      };
    }
  });

  return formValues;
}
