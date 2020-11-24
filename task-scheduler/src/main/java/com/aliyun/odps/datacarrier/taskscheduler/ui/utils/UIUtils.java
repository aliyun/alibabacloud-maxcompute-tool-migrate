/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.datacarrier.taskscheduler.ui.utils;

import static j2html.TagCreator.a;
import static j2html.TagCreator.body;
import static j2html.TagCreator.div;
import static j2html.TagCreator.h3;
import static j2html.TagCreator.header;
import static j2html.TagCreator.html;
import static j2html.TagCreator.li;
import static j2html.TagCreator.link;
import static j2html.TagCreator.meta;
import static j2html.TagCreator.p;
import static j2html.TagCreator.script;
import static j2html.TagCreator.span;
import static j2html.TagCreator.strong;
import static j2html.TagCreator.title;
import static j2html.TagCreator.ul;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.AbstractActionInfo;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.HiveSqlActionInfo;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.OdpsSqlActionInfo;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.VerificationActionInfo;
import com.aliyun.odps.datacarrier.taskscheduler.ui.WebUITab;
import com.aliyun.odps.utils.StringUtils;
import j2html.tags.DomContent;

public class UIUtils {
  private static final DateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

  public String decodeURLParameter(String urlParam) throws UnsupportedEncodingException {
    return URLDecoder.decode(urlParam, "UTF-8");
  }

  public static String formatDate(Long timestamp) {
    if (timestamp == null) {
      return "N/A";
    }
    return DEFAULT_DATE_FORMAT.format(new Date(timestamp));
  }

  public static String formatDuration(Long startTime, Long endTime) {
    if (startTime == null) {
      return "N/A";
    }

    Long ms;
    if (endTime == null) {
      ms = System.currentTimeMillis() - startTime;
    } else {
      ms = endTime - startTime;
    }

    if (ms < 100){
      return String.format("%d ms", ms);
    }
    Double seconds = ms.doubleValue() / 1000;
    if (seconds < 1) {
      return String.format("%.1f s", seconds);
    }
    if (seconds < 60) {
      return String.format("%.0f s", seconds);
    }
    Double minutes = seconds / 60;
    if (minutes < 10) {
      return String.format("%.1f min", minutes);
    }
    if (minutes < 60) {
      return String.format("%.0f min", minutes);
    }
    Double hours = minutes / 60;
    return String.format("%.1f h", hours);
  }

  public static DomContent actionInfoEntry(String key, String val) {
    if (val == null) {
      val = "N/A";
    }

    return li(
        strong(key + ": "),
        span(val).withStyle("word-break: break-all;")
    );
  }

  public static DomContent actionInfoEntry(String key, DomContent val) {
    if (val == null) {
      val = span("N/A");
    }

    return li(
        strong(key + ": "),
        val
    );
  }

  public static DomContent actionInfoTable(Action action) {
    AbstractActionInfo actionInfo = action.getActionInfo();

    if (actionInfo instanceof HiveSqlActionInfo) {
      HiveSqlActionInfo hiveSqlActionInfo = (HiveSqlActionInfo) actionInfo;

      List<DomContent> listEntries = new LinkedList<>();

      String jobId = hiveSqlActionInfo.getJobId();
      if (!StringUtils.isNullOrEmpty(jobId)) {
        listEntries.add(
            actionInfoEntry("Hive job ID", hiveSqlActionInfo.getJobId())
        );
      } else {
        listEntries.add(
            actionInfoEntry("Hive job name", action.getId())
        );
      }

      String trackingUrl = hiveSqlActionInfo.getTrackingUrl();
      if (!StringUtils.isNullOrEmpty(trackingUrl)) {
        listEntries.add(
            actionInfoEntry("Hive job tracking URL",
                            a(hiveSqlActionInfo.getTrackingUrl()).withStyle("word-break: break-all;"))
        );
      }
      // TODO: progress

      return ul(
        listEntries.toArray(new DomContent[0])
      );
    } else if (actionInfo instanceof OdpsSqlActionInfo) {
      OdpsSqlActionInfo odpsSqlActionInfo = (OdpsSqlActionInfo) actionInfo;
      List<DomContent> listEntries = new LinkedList<>();
      listEntries.add(
          actionInfoEntry("MC instance ID", odpsSqlActionInfo.getInstanceId())
      );
      if (odpsSqlActionInfo.getLogView() != null) {
        listEntries.add(
            actionInfoEntry("MC instance tracking URL",
                            a(odpsSqlActionInfo.getLogView()).withStyle("word-break: break-all;"))
        );
      } else {
        listEntries.add(
            actionInfoEntry("MC instance tracking URL", "N/A")
        );
      }
      // TODO: progress

      return ul(
          listEntries.toArray(new DomContent[0])
      );
    } else if (actionInfo instanceof VerificationActionInfo) {
      VerificationActionInfo verificationActionInfo = (VerificationActionInfo) actionInfo;

      if (verificationActionInfo.isPartitioned() == null) {
        return null;
      } else if (verificationActionInfo.isPartitioned()) {
        List<DomContent> listEntries = new LinkedList<>();
        if (verificationActionInfo.passed() != null) {
          listEntries.add(
              actionInfoEntry("Passed", verificationActionInfo.passed().toString())
          );
          listEntries.add(
              actionInfoEntry(
                  "Number of succeeded partitions",
                  Integer.toString(verificationActionInfo.getSucceededPartitions().size()))
          );
          // Remove later
          if (verificationActionInfo.getSucceededPartitions().size() != 0) {
            listEntries.add(
                li(
                    strong("Succeeded partitions: "),
                    ul(
                        verificationActionInfo
                            .getSucceededPartitions()
                            .stream()
                            .map(row -> actionInfoEntry("Partition values", String.join(", ", row)))
                            .toArray(DomContent[]::new)
                    )
                )
            );
          }
          listEntries.add(
              actionInfoEntry(
                  "Number of failed partitions",
                  Integer.toString(verificationActionInfo.getFailedPartitions().size()))

          );
          if (verificationActionInfo.getFailedPartitions().size() != 0) {
            listEntries.add(
                li(
                    strong("Failed partitions: "),
                    ul(
                        verificationActionInfo
                            .getFailedPartitions()
                            .stream()
                            .map(row -> actionInfoEntry("Partition values", String.join(", ", row)))
                            .toArray(DomContent[]::new)
                    )
                )
            );
          }
        } else {
          listEntries.add(
              actionInfoEntry("Passed", "N/A")
          );
        }

        return ul(
            listEntries.toArray(new DomContent[0])
        );
      } else {
        List<DomContent> listEntries = new LinkedList<>();
        if (verificationActionInfo.passed() != null) {
          listEntries.add(
              actionInfoEntry("Passed", verificationActionInfo.passed().toString())
          );
        } else {
          listEntries.add(
              actionInfoEntry("Passed", "N/A")
          );
        }
        return ul(
            listEntries.toArray(new DomContent[0])
        );
      }
    } else {
      return null;
    }
  }

  public static String prependBaseUri(String basePath, String resource) {
    return basePath + "/" + resource + "/";
  }

  public static String basicMmaPage(String title, List<DomContent> content, WebUITab activeTab) {
    List<DomContent> tags = new LinkedList<>();

    tags.add(
        div(
            div(
                div(
                    p(
                        strong("MC Migration Assistant")
                    ).withClass("navbar-text pull-left")
                ).withClass("brand"),
                ul(
                    activeTab.getHeaderTabs().stream().map(tab ->
                        li(
                            a(tab.getPrefix().toUpperCase())
                                .withHref(prependBaseUri(tab.getBasePath(), tab.getPrefix()))
                        ).withClass(tab == activeTab ? "active" : "")
                    ).toArray(DomContent[]::new)
                ).withClass("nav")
            ).withClass("navbar-inner")
        ).withClass("navbar navbar-static-top")
    );

    content.add(0,
        div(
            div(
                h3(title).withStyle("vertical-align: bottom; display: inline-block;")
            ).withClass("span12")
        ).withClass("row-fluid")
    );
    tags.add(
        div(
            content.toArray(new DomContent[0])
        ).withClass("container-fluid")
    );

    return html(
        header(
            title(title),
            meta().attr("http-equiv", "Content-type")
                  .withContent("text/html")
                  .withCharset("utf-8"),
            meta().attr("http-equiv", "refresh").withContent("30"),
              //TODO: icon not working
            link().withRel("icon")
                  .withHref("/static/favicon-16x16.png")
                  .withType("image/png"),
            link().withRel("stylesheet")
                  .withHref("/static/bootstrap.min.css")
                  .withType("text/css"),
            link().withRel("stylesheet")
                  .withHref("/static/vis.min.css")
                  .withType("text/css"),
            link().withRel("stylesheet")
                  .withHref("/static/webui.css")
                  .withType("text/css"),
            link().withRel("stylesheet")
                  .withHref("/static/timeline-view.css")
                  .withType("text/css"),
            link().withRel("stylesheet")
                  .withHref("/static/dag-viz.css")
                  .withType("text/css"),
            script().withSrc("/static/jquery-1.11.1.min.js"),
            script().withSrc("/static/vis.min.js"),
            script().withSrc("/static/bootstrap-tooltip.js"),
            script().withSrc("/static/initialize-tooltips.js"),
            script().withSrc("/static/webui.js"),
            script().withSrc("/static/d3.min.js"),
            script().withSrc("/static/dag-viz.js"),
            script().withSrc("/static/dagre-d3.min.js"),
            script().withSrc("/static/graphlib-dot.min.js")
        ),
        body(
            tags.toArray(new DomContent[0])
        )
    ).render();
  }
}
