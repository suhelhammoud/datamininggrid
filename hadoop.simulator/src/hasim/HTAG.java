package hasim;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_predicate;
import eduni.simjava.Sim_type_p;

import gridsim.GridSimTags;

/**
 * Logger for this class
 */
public enum HTAG {
	outside,
	fileWriteRequest, fileReadRequest, WRITEFILE_COMPLETE, READFILE_COMPLETE,

	writeDelta, readDelta, writing, reading, completeWriting, completeReading,

	lastTag, fileReadComplete, fileWriteComplete, update, 
	
	mem_write, mem_sort, mem_threshold, mem_full, mem_combine, mem_spill, 
	
	newtag,
	
	heartbeat,heartbeat_request, heartbeat_response,
	
	new_map_task, new_reduce_task, new_shuffle_task,
	
	
	get_split_datum_response,
	 get_split_datum_request,
	
	cpu_map_response,
	mem_sort_request, mem_sort_complete, 
	cpu_local_submit, cpu_tic, 
	
	joblet_progress, joblet_complete, joblet_submit_local, 
	joblet_part, joblet_part_return, joblet_complete_return,joblet_submitPart_complete_return,
	
	netlet_send, netlet_send_part,netlet_receive,netlet_complete, 
	 netlet_part_return, netlet_complete_return, netlet_send_return,   
	 
	file_submit, file_complete, file_part,file_complete_return, file_part_return, 
	mapper_start_task_local, 
	
	mapper_block, mapper_unblock,
	datum_complete_return, datum_part_return, 
	mem_add, mem_add_return, mem_set_mapper, mem_set_spiller,mem_flush, 
	spiller_start, mem_reset, spiller_end,
	mem_add_return_true,mem_add_return_false, 
	read_tmp_delta,write_tmp_delta,process_tmp_delta, send_tmp_delta,
	read_tmp_delta_return, write_tmp_delta_return, process_tmp_delta_return,
	send_tmp_delta_return, 
	hdd_add, hdd_remove, hdd_check,
	cpu, cpu_add, cpu_check, 
	
	job_tracker_add_job, 
	START, mapper_cpu,

	;

	public static final int HDBASE = 1000;
	private final int tagId;

	public static String toString(int i){
		HTAG tag=get(i);
		return tag==HTAG.outside?""+i:tag.name();
	}
	public static HTAG get(int i) {
		int dif = i - HDBASE;
		HTAG[] arr = HTAG.values();
		if (dif < 0 || dif > arr.length)
			return HTAG.outside;
		return HTAG.values()[dif];
	}

	private HTAG() {
		this.tagId = ordinal() + HDBASE;
	}

	public int id() {
		return tagId;
	}

	public String tagName(int tagId) {
		int d = (HDBASE - tagId);
		return "" + HTAG.values()[d].name();
	}

	public Sim_predicate predicate(){
		return new Sim_type_p(id());
	}
	
	public static void main(String[] args) {
		HTAG tag = HTAG.READFILE_COMPLETE;

		System.out.println("" + tag + "=" + tag.id());
		System.out.println("reverse " + HTAG.get(tag.id()));
	}

}
