package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.io.Serializable;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "swx", name = "mrtadmin_participant_dim",
    readConsistency = "QUORUM",
    writeConsistency = "QUORUM",
    caseSensitiveKeyspace = false,
    caseSensitiveTable = false)
public class Participant implements Serializable {
	private static final long serialVersionUID = 7913182765371168889L;

    @PartitionKey
	@Column(name = "participant_dim_key")
    private Long participantDimKey;

    @ClusteringColumn
	@Column(name = "participant_short_name")
    private String participantShortName;

    @Column(name = "participant_abbrev")
    private String participantAbbrev;
    
    @Column(name = "participant_id")
    private Long participantId;
    
	public Long getParticipantDimKey() {
		return participantDimKey;
	}

	public void setParticipantDimKey(Long participantDimKey) {
		this.participantDimKey = participantDimKey;
	}

	public String getParticipantShortName() {
		return participantShortName;
	}

	public void setParticipantShortName(String participantShortName) {
		this.participantShortName = participantShortName;
	}

	public Long getParticipantId() {
		return participantId;
	}

	public void setParticipantId(Long participantId) {
		this.participantId = participantId;
	}

	public String getParticipantAbbrev() {
		return participantAbbrev;
	}

	public void setParticipantAbbrev(String participantAbbrev) {
		this.participantAbbrev = participantAbbrev;
	}
  
}
