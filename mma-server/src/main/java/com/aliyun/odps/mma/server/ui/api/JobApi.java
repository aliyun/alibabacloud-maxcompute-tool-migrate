/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mma.server.ui.api;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.crypto.NoSuchPaddingException;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.JobInfoOutputsV1;
import com.aliyun.odps.mma.config.TaskInfoOutputs;
import com.aliyun.odps.mma.config.OutputsWrapper;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.JobScheduler;
import com.aliyun.odps.mma.server.config.MmaServerConfiguration;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.server.job.JobManager;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.ui.utils.ApiUtils;
import com.aliyun.odps.mma.util.Constants;
import com.aliyun.odps.mma.util.GsonUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class JobApi extends AbstractRestfulApi {

  private static final Logger LOG = LogManager.getLogger(JobApi.class);

  private JobScheduler jobScheduler;
  private JobManager jobManager;

  public JobApi(String prefix, JobManager jobManager, JobScheduler jobScheduler) throws MmaException {
    super(prefix);
    this.jobManager = Objects.requireNonNull(jobManager);
    this.jobScheduler = Objects.requireNonNull(jobScheduler);
  }

  @Override
  public void handleDelete(
      HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {
    // Handle parameters
    String parameterJobId = request.getParameter(Constants.JOB_ID_PARAM);
    String parameterPermanent = request.getParameter(Constants.PERMANENT_PARAM);
    if (parameterJobId == null) {
      ApiUtils.handleError(
          response,
          HttpServletResponse.SC_BAD_REQUEST,
          "Missing required parameter: " + Constants.JOB_ID_PARAM);
      return;
    }
    boolean permanent = parameterPermanent == null ? false : Boolean.valueOf(parameterPermanent);
    LOG.info("Method: DELETE, job id: {}, permanent: {}", parameterJobId, permanent);

    // Stop job
    Job job;
    try {
      job = jobManager.getJobById(parameterJobId);
    } catch (Exception e) {
      if (e instanceof IllegalArgumentException) {
        ApiUtils.handleError(response, HttpServletResponse.SC_NOT_FOUND, e);
        return;
      } else {
        throw new ServletException(e);
      }
    }

    try {
      job.stop();
    } catch (Exception e) {
      throw new ServletException(e);
    }

    if (permanent) {
      jobManager.removeJob(parameterJobId);
    }

    response.setStatus(HttpServletResponse.SC_OK);
  }

  @Override
  public void handleGet(
      HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {
    String parameterJobId = request.getParameter(Constants.JOB_ID_PARAM);
    String parameterSubJobId = request.getParameter(Constants.SUB_JOB_ID_PARAM);
    LOG.info("Method: GET, job id: {}", parameterJobId);
    if (parameterJobId == null) {
      // List jobs
      List<Job> jobs = jobManager.listJobs();
      response.getWriter().write(getJobListOutputs(jobs));
    } else {
      // Get job info
      Job job;
      try {
        job = jobManager.getJobById(parameterJobId);
        
        if(job.hasSubJob() && parameterSubJobId !=null) {
        	job = jobManager.getSubJobById(job, parameterSubJobId);
        }
      } catch (Exception e) {
        if (e instanceof IllegalArgumentException) {
          ApiUtils.handleError(response, HttpServletResponse.SC_NOT_FOUND, e);
          return;
        } else {
          throw new ServletException(e);
        }
      }
      response.getWriter().write(getJobInfoOutputs(job));
    }
    response.setStatus(HttpServletResponse.SC_OK);
  }

  private String getJobListOutputs(List<Job> jobs) {
    OutputsWrapper<List<JobInfoOutputsV1>> wrapper = new OutputsWrapper<>();
    wrapper.setProtocolVersion(1);
    List<JobInfoOutputsV1> jobInfoOutputs = jobs
        .stream()
        .map(job -> {
          JobConfiguration config = job.getJobConfiguration();
          return new JobInfoOutputsV1(
              job.getId(),
              config.get(JobConfiguration.OBJECT_TYPE),
              config.get(JobConfiguration.SOURCE_CATALOG_NAME),
              config.get(JobConfiguration.SOURCE_OBJECT_NAME),
              config.get(JobConfiguration.DEST_CATALOG_NAME),
              config.get(JobConfiguration.DEST_OBJECT_NAME),
              job.getPriority(),
              job.getCreationTime(),
              job.getStartTime(),
              job.getEndTime(),
              job.getStatus().name(),
              // TODO: progress
              0.0D,
              null,
              null);
        })
        .collect(Collectors.toList());
    wrapper.setOutputs(jobInfoOutputs);
    return GsonUtils.GSON.toJson(wrapper);
  }

  private String getJobInfoOutputs(Job job) {
	List<JobInfoOutputsV1> jobInfoOutputs = null;
	List<TaskInfoOutputs> taskInfoOutputs = null;
	try {
		if(job.hasSubJob()) {
			jobInfoOutputs = job.getSubJobs()
		          .stream()
		          .map(sjob -> {
		            JobConfiguration sconfig = sjob.getJobConfiguration();
		            return new JobInfoOutputsV1(
		                sjob.getId(),
		                sconfig.get(JobConfiguration.OBJECT_TYPE),
		                sconfig.get(JobConfiguration.SOURCE_CATALOG_NAME),
		                sconfig.get(JobConfiguration.SOURCE_OBJECT_NAME),
		                sconfig.get(JobConfiguration.DEST_CATALOG_NAME),
		                sconfig.get(JobConfiguration.DEST_OBJECT_NAME),
		                job.getPriority(),
		                job.getCreationTime(),
		                job.getStartTime(),
		                job.getEndTime(),
		                sjob.getStatus().name(),
		                // TODO: progress
		                0.0D,null,null);
		          })
		          .collect(Collectors.toList());
		}
		else {
			taskInfoOutputs = job.getTasks()
			          .stream()
			          .map(stask -> {
			            return new TaskInfoOutputs(
			                stask.getId(),
			                stask.getStartTime(),
			                stask.getEndTime(),
			                stask.getProgress().toString());
			          })
			          .collect(Collectors.toList());
			
		}
	} catch(Exception e) {
			
	}
    OutputsWrapper<JobInfoOutputsV1> wrapper = new OutputsWrapper<>();
    wrapper.setProtocolVersion(1);
    JobConfiguration config = job.getJobConfiguration();
    JobInfoOutputsV1 outputs = new JobInfoOutputsV1(
        job.getId(),
        config.get(JobConfiguration.OBJECT_TYPE),
        config.get(JobConfiguration.SOURCE_CATALOG_NAME),
        config.get(JobConfiguration.SOURCE_OBJECT_NAME),
        config.get(JobConfiguration.DEST_CATALOG_NAME),
        config.get(JobConfiguration.DEST_OBJECT_NAME),
        job.getPriority(),
        job.getCreationTime(),
        job.getStartTime(),
        job.getEndTime(),
        job.getStatus().name(),
        // TODO: progress
        0.0D,
        jobInfoOutputs,
        taskInfoOutputs
        );
    wrapper.setOutputs(outputs);
    return GsonUtils.GSON.toJson(wrapper);
  }

  @Override
  public void handlePost(
      HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {

    LOG.info("Method: POST");

    JobConfiguration config = getJobConfiguration(request);
    try {
      config.validate();
    } catch (MmaException e) {
      ApiUtils.handleError(
          response,
          HttpServletResponse.SC_BAD_REQUEST,
          e);
      return;
    }

    String jobId;
    try {
      jobId = jobManager.addJob(config);
    } catch (Exception e) {
      throw new ServletException(e);
    }

    Job job;
    try {
      job = jobManager.getJobById(jobId);
    } catch (Exception e) {
      if (e instanceof IllegalArgumentException) {
        ApiUtils.handleError(response, HttpServletResponse.SC_NOT_FOUND, e);
        return;
      } else {
        throw new ServletException(e);
      }
    }

    jobScheduler.schedule(job);

    OutputsWrapper<JobInfoOutputsV1> wrapper = new OutputsWrapper<>();
    wrapper.setProtocolVersion(1);
    JobInfoOutputsV1 outputs = new JobInfoOutputsV1(
        jobId,
        config.get(JobConfiguration.OBJECT_TYPE),
        config.get(JobConfiguration.SOURCE_CATALOG_NAME),
        config.get(JobConfiguration.SOURCE_OBJECT_NAME),
        config.get(JobConfiguration.DEST_CATALOG_NAME),
        config.get(JobConfiguration.DEST_OBJECT_NAME),
        job.getPriority(),
        job.getCreationTime(),
        job.getStartTime(),
        job.getEndTime(),
        JobStatus.PENDING.name(),
        // TODO: progress
        0.0D,
        null,
        null);
    wrapper.setOutputs(outputs);

    response.getWriter().write(GsonUtils.GSON.toJson(wrapper));
    response.setStatus(HttpServletResponse.SC_OK);
  }

  @Override
  public void handlePut(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String parameterJobId = request.getParameter(Constants.JOB_ID_PARAM);
    if (StringUtils.isBlank(parameterJobId)) {
      ApiUtils.handleError(
          response,
          HttpServletResponse.SC_BAD_REQUEST,
          "Missing required parameter: " + Constants.JOB_ID_PARAM);
      return;
    }
    String parameterAction = request.getParameter(Constants.ACTION_PARAM);
    if (StringUtils.isBlank(parameterAction)) {
      ApiUtils.handleError(
          response,
          HttpServletResponse.SC_BAD_REQUEST,
          "Missing required parameter: " + Constants.ACTION_PARAM);
      return;
    }

    switch (parameterAction) {
      case Constants.RESET_ACTION: {
        String parameterForce = request.getParameter(Constants.FORCE_PARAM);
        boolean force = parameterForce == null ? false : Boolean.valueOf(parameterForce);
        LOG.info("Method: PUT, job id: {}, force: {}", parameterJobId, force);

        Job job;
        try {
          job = jobManager.getJobById(parameterJobId);
        } catch (Exception e) {
          if (e instanceof IllegalArgumentException) {
            ApiUtils.handleError(response, HttpServletResponse.SC_NOT_FOUND, e);
            return;
          } else {
            throw new ServletException(e);
          }
        }

        try {
          job.reset(force);
        } catch (Exception e) {
          throw new ServletException(e);
        }
        jobScheduler.schedule(job);
        break;
      }
      case Constants.UPDATE_ACTION: {
        // TODO
        JobConfiguration config = getJobConfiguration(request);
        break;
      }
      default:
        throw new IllegalArgumentException("Unknown action: " + parameterAction);
    }
  }

  private JobConfiguration getJobConfiguration(HttpServletRequest request)
      throws IOException, ServletException {
    ServletInputStream im = request.getInputStream();
    byte[] bytes = IOUtils.toByteArray(im);
    JsonParser parser = new JsonParser();
    JsonObject body = parser.parse(new String(bytes, StandardCharsets.UTF_8)).getAsJsonObject();

    if (!body.has("ProtocolVersion")) {
      throw new IllegalStateException("Missing required member 'ProtocolVersion'");
    }
    int protocolVersion = body.get("ProtocolVersion").getAsInt();
    LOG.debug("Protocol version: {}", protocolVersion);

    String decryptedInputs;
    try {
      decryptedInputs = getDecryptedInputs(body);
    } catch (NoSuchPaddingException
        | java.security.NoSuchAlgorithmException
        | java.security.InvalidKeyException
        | javax.crypto.BadPaddingException
        | javax.crypto.IllegalBlockSizeException e) {
      throw new ServletException(e);
    }

    Map<String, String> baseConfig = new HashMap<>(MmaServerConfiguration.getInstance());
    JobConfiguration config = JobConfiguration.fromJson(decryptedInputs);
    for (Entry<String, String> entry : config.entrySet()) {
      baseConfig.put(entry.getKey(), entry.getValue());
    }
    return new JobConfiguration(baseConfig);
  }
}
