[wdai11@argon-login-2 04_Stability]$ nohup ./jobcheck2.py &
[3] 37649

***** 2018-10-18   16:32:52 *****

In the folder "03_Test", I will build the new version of jobcheck. The
important updates include:

1. Support for parallel meep

2. Find the home directory for each job in the queue.


***** 2018-10-18   16:35:45 *****

Qjob: job from myq
idx,status,btime,server,slots,folder
short_str: don't shown folder

Qjob_list
qjobs
Methods: append, checkstatus(idx),
         qjob=find(idx)
         [qjobs]=find_base_folder(folder)
         UI_usage: UI jobs
         n_jobs: total jobs
         n_rjobs: total running jobs
         myq: build the list based on qstat result
	 short_str()


Fjob: job from folders
folder,status,message
Class method: create_done_job(path)
      	      create_not_done_job(path)
	      create_run_job(qjob,path)
	      create_wait_job(qjob,path)
	      create_error_job(qjob,path)
              create_unknown_job(qjob,path)
	      create_plural_job(qjobs,path)

Fjob_list
fjobs
two action constant: n_del, n_sub
Methods:  walk_and_build(path): walk the path and build the list
	  dict_jobs(self):		
	  summary(): retrun a string with all the information
	  info_normal_jobs(self,status): 'r','w','d','nd'



Collect information from job files:
jobid_from_begin_file()
jobid_from_done_file()

is_finished_from_job_file()
is_finished_from_dat_file()
job_done_id()
ead_job_info(idx):


Help function:
     character_frame(word)
