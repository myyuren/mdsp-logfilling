package com.pzoom.mdsp.logfilling;


import com.google.common.base.Objects;

/** 
 * kafka 分区对象
 * @author chenbaoyu
 *
 */
public class Partition {
	
	public final String bokerInfo;
	public final int partition;

	public Partition(String bokerInfo, int partition) {
		this.bokerInfo = bokerInfo;
		this.partition = partition;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(bokerInfo, partition);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		final Partition other = (Partition) obj;
		return Objects.equal(this.bokerInfo, other.bokerInfo) && Objects.equal(this.partition, other.partition);
	}

	@Override
	public String toString() {
		return "Partition{" +
				"bokerInfo=" + bokerInfo +
				", partition=" + partition +
				'}';
	}

	public String getId() {
		return "partition_" + partition;
	}

}
