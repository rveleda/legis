package ca.yorku.asrl.legis.server;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement 
@XmlAccessorType(XmlAccessType.FIELD)
public class SparkSubmitResponse {
	
	private String action;
	
	private String message;
	
	private String serverSparkVersion;
	
	private String submissionId;
	
	private boolean success;

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getServerSparkVersion() {
		return serverSparkVersion;
	}

	public void setServerSparkVersion(String serverSparkVersion) {
		this.serverSparkVersion = serverSparkVersion;
	}

	public String getSubmissionId() {
		return submissionId;
	}

	public void setSubmissionId(String submissionId) {
		this.submissionId = submissionId;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}
}
