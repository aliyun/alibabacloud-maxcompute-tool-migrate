package com.aliyun.odps.mma.server.config;

import com.aliyun.odps.mma.server.event.MmaEventSenderType;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class MmaEventSenderConfiguration {
  @Expose
  @SerializedName("Type")
  private MmaEventSenderType type;

  @Expose
  @SerializedName("WebhookUrl")
  private String webhookUrl;

  public MmaEventSenderType getType() {
    return type;
  }

  public String getWebhookUrl() {
    return webhookUrl;
  }
}
