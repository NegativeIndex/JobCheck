#!/Users/wdai11/anaconda3/bin/python3

import sys
import subprocess
import datetime 
import re
import os,glob,time
import random
from apscheduler.schedulers.blocking import BlockingScheduler

sys.path.insert(0,'/Users/wdai11/function')
import my_output as my

##################################################
# collect information from job files
###################################################
def jobid_from_begin_file(path='./'):
    fname=os.path.join(path,'job.begin')
    if not os.path.isfile(fname):
        return None

    idxes=[]
    with open(fname) as fp:
        for line in fp:
            m=re.search(r'Your job (\d+)',line)
            if m:
                idxes.append(m.group(1))
    
    return idxes
    

def jobid_from_done_file(path='./'):
    fname=os.path.join(path,'job.done')
    if not os.path.isfile(fname):
        return None

    idxes=[]
    with open(fname) as fp:
        for line in fp:
            m=re.search(r'^(\d+) ',line)
            if m:
                idxes.append(m.group(1))
    
    return idxes    


def is_finished_from_job_file(path='./'):
    idx1=jobid_from_begin_file(path)
    idx2=jobid_from_done_file(path)
    if idx1 and idx2 and idx1[-1]==idx2[-1]:
        return True
    else:
        return False


def is_finished_from_dat_file(path='./'):
    cwd = os.getcwd()
    os.chdir(path)    
    files= glob.glob("*.dat")
    os.chdir(cwd)
    if files:
        return True
    else:
        return False

def job_done_id(path='./'):
    idx2=jobid_from_done_file(path)
    return idx2[-1]
    
# collect info from job.info
def read_job_info(idx,path='./'):
    info=(idx,)
    fname=os.path.join(path,"job.info")
    if os.path.isfile(fname):
        with open(fname) as f:
            lines=f.readlines()
    else:
        return info

    # get btime, job begin time
    for i in range(len(lines)-2):
        matchObj1=re.match( r'^[+]+$', lines[i], re.M|re.I)
        matchObj2=re.match( '^'+idx, lines[i+2], re.M|re.I)
        if matchObj1 and matchObj2:
            line=lines[i+1]
            ss=line.split()
            newline="{}/{}/{} {}".format(ss[1],ss[2],ss[5],ss[3])
            btime=datetime.datetime.strptime(newline, "%b/%d/%Y %H:%M:%S")
            info+=(btime,)

    for i in range(len(lines)-2):
        matchObj1=re.match( r'^[-]+$', lines[i], re.M|re.I)
        matchObj2=re.match( '^'+idx, lines[i+2], re.M|re.I)
        if matchObj1 and matchObj2:
            line=lines[i+1]
            ss=line.split()
            newline="{}/{}/{} {}".format(ss[1],ss[2],ss[5],ss[3])
            etime=datetime.datetime.strptime(newline, "%b/%d/%Y %H:%M:%S")
            info+=(etime,)

    return info


##################################################
# actions
###################################################
def kill_job(idx):
    comm=["qdel",idx]
    res = subprocess.check_output(comm)
    print("Killing message: "+res)
    time.sleep(3)

####################################
def touch(path='./'):
    basedir = os.path.dirname(path)
    if not os.path.exists(basedir):
        os.makedir(basedir)

    with open(path, 'a'):
        os.utime(path, None)

####################################
def submit_job(server="all.q",smp=None,path='./'):
    cwd = os.getcwd()
    os.chdir(path) 
    files= glob.glob("dwt*.job")
    for fname in  files:
        comm1=["-q", server]
        res=subprocess.check_output(["qsub"]+comm1+[fname])
        line=res.decode("utf-8")
        print(line)
        with open("job.begin", "a+") as f:
            f.write(line)
    os.chdir(cwd)

##################################################
# help function
##################################################
def character_frame(word):
    return '#'*50+'\n# '+word+'\n'+'#'*50
   

##################################################
# define a class to collect information from qstat
###################################################
class Qjob:
    def __init__(self,idx,status,btime,server,slots,folder):
        self.idx = idx
        self.status = status 
        self.btime=btime
        self.server=server
        self.slots=slots
        self.folder=folder

    def __str__(self):
        ss="{:>7} {:>3} {:>6}   {:20} {:2}\n{}".format(
            self.idx,self.status,self.server,
            self.btime.strftime("%m/%d/%Y %H:%M:%S"),
            self.slots,
            self.folder)
        return  ss     

    def short_str(self):
        ss="{:>7} {:>3} {:>6}   {:20} {:2}".format(
            self.idx,self.status,self.server,
            self.btime.strftime("%m/%d/%Y %H:%M:%S"),
            self.slots)
        return  ss     

class Qjob_list:
    def __init__(self):
        self.qjobs=[]

        
    def __str__(self):
        ss=""
        for qjob in self.qjobs:
            ss=ss+str(qjob)+"\n"
        return ss.rstrip()

    def short_str(self):
        ss=""
        for qjob in self.qjobs:
            ss=ss+qjob.short_str()+"\n"
        return ss.rstrip()


    def append(self,qjob):
        self.qjobs.append(qjob)
        
    # check the status givin idx
    def checkstatus(self,idx):
        for job in self.qjobs:
            if idx==job.idx:
                return job.status
        return "n"
        
    # find a job based on idx    
    def find(self,idx):
        for job in self.qjobs:
            if idx==job.idx:
                return job
        return None
        
    # find a job based on the folder
    def find_base_folder(self,folder):
        jobs_list=[job for job in self.qjobs if \
                   os.path.abspath(job.folder)==os.path.abspath(folder)]
        return jobs_list

    # how many UI used    
    def UI_usage(self):
        n=0
        for job in self.qjobs:
            if job.server=="UI":
                n+=1
        return n

    # how many jobs submitted
    def n_jobs(self):
        return len(self.qjobs)
    
    # how many jobs running
    def n_rjobs(self):
        n=0
        for job in self.qjobs:
            if job.status=="r":
                n+=1
        return n

    # update itself based on myq results
    def myq(self):
        res = subprocess.check_output("myq")
        lines=res.splitlines()
        del lines[0:2]

        for ll in lines:
            line=ll.decode("utf-8")
            words=line.split()
            idx=words[0]
            status=words[4]
            btime_str=words[5]+" "+words[6]
            queue=words[7]
            slots=words[8]
            btime=datetime.datetime.strptime(btime_str, "%m/%d/%Y %H:%M:%S")
            
            matchObj = re.match( r'(.*)@', queue, re.M|re.I)
            if matchObj:
                server=matchObj.group(1)
            else:
                server="all.q"
            # get folder
            res = subprocess.check_output(['qstat','-j',idx])
            match=re.search(r'sge_o_workdir:\s+(\S+)\s+',res.decode("utf-8"))
            if match:
                folder=match.group(1)
            else:
                folder=None
            qjob=Qjob(idx,status,btime,server,slots,folder)
            self.append(qjob)


##########################################
# define a class for jobs from folders
###########################################
# check status,
# qw: wait
# r: good and wait
# eqw: kill and resubmit
# n: not running, check folder, update to d or nd
# d: done, good
# nd: not done and not running, resubmit
class Fjob: 
    def __init__(self,folder,status,message):
        self.folder = folder
        self.status = status 
        self.message = message
    
    def __str__(self):
        ss="------------------------------------------\n{}\n{}".format(
            self.folder,self.message)
        return  ss   

    # class method, to create all kinds of Fjob
    def create_done_job(path):
        status='d'
        idx=job_done_id(path)
        info=read_job_info(idx,path)
        btime=info[1]
        etime=info[2]
        dtime=etime-btime
        dtime_str=my.nice_sec2str(dtime.total_seconds())
        ss1="Done"
        ss2="Simulation time "+dtime_str
        message=ss1+"\n"+ss2
        return Fjob(path,status,message)

    def create_not_done_job(path):
        status='nd'
        ctime=datetime.datetime.now()
        ctime_str=datetime.datetime.strftime(ctime,
                                             "%m/%d/%Y %H:%M:%S")
        ss1='Not running'
        ss2="Checked at "+ctime_str
        message=ss1+"\n"+ss2
        return Fjob(path,status,message)

    def create_run_job(qjob,path):
        status='r'
        idx=qjob.idx
        info=read_job_info(idx,path)
        stime=qjob.btime  # job submission time
        btime=info[1]     # job beginning time
        ctime=datetime.datetime.now() # current time
        dtime=ctime-btime  # job running time

        ctime_str=datetime.datetime.strftime(ctime,
                                    "%m/%d/%Y %H:%M:%S")
        dtime_str=my.nice_sec2str(dtime.total_seconds())
        ss1="Running     "+idx
        ss2="Checked at "+ctime_str
        ss3="Running time: "+dtime_str
        message=ss1+"\n"+ss2+"\n"+ss3
        return Fjob(path,status,message)

    def create_wait_job(qjob,path):
        status="qw"
        idx=qjob.idx
        ss1="Waiting     "+idx
        ctime=datetime.datetime.now() # current time
        stime=qjob.btime  # job submission time      
        dtime=ctime-stime

        ctime_str=datetime.datetime.strftime(ctime,
                                 "%m/%d/%Y %H:%M:%S")
        dtime_str=my.nice_sec2str(dtime.total_seconds())
        ss2="Checked at "+ctime_str
        ss3="Waiting time: "+dtime_str
        message=ss1+"\n"+ss2+"\n"+ss3
        return Fjob(path,status,message)

    def create_error_job(qjob,path):
        status="Eqw"
        idx=qjob.idx
        ctime=datetime.datetime.now()
        ctime_str=datetime.datetime.strftime(ctime,
                                             "%m/%d/%Y %H:%M:%S")
    
        ss1="Error     "+idx+" Will kill and resubmit"
        ss2="Checked at "+ctime_str
        message=ss1+"\n"+ss2
        return Fjob(path,status,message)             

    def create_unknown_job(qjob,path):
        status=qjob.status
        idx=qjob.idx
        message="Unknown status ({}) of  {}".format(status,idx)
        return Fjob(path,status,message)             

    def create_plural_job(qjobs,path):
        status='p'
        message="More than one jobs in the folder"
        for job in qjobs:
            message+='\n'+str(job)
        return Fjob(path,status,message)    


class Fjob_list:
    def __init__(self):
        self.fjobs=[]
        self.n_del=0
        self.n_sub=0
       
    def __str__(self):
        ss=""
        for job in self.fjobs:
            if job.status!='d': 
                ss=ss+str(job)+"\n"
        ss=ss[:-1]
        return ss
    def append(self,sjob):
        self.fjobs.append(sjob) 

    def dict_jobs(self):
        all_status=set(('d','nd','r','qw','Eqw','p','un'))
        dict={key:0 for key in all_status}
        for job in self.fjobs:
            if job.status in all_status:
                dict[job.status]+=1
            else:
                dict['un']+=1
        return dict

    def summary(self):
        n=len(self.fjobs)
        dict=self.dict_jobs()
        ss='There are {} folders: \n'.format(n)
        ss+='{} running, {} waiting, {} finished\n'.format(
            dict['r'],dict['qw'],dict['d'])
        if dict['nd']>0:
            ss+='{} not done, resubmit them\n'.format(dict['nd'])
        if dict['Eqw']>0:
            ss+='{} are Eqw, kill and resubmit them\n'.format(dict['Eqw'])
        if dict['p']>0:
            ss+='{} folders have plural jobs\n'.format(dict['p'])
        if dict['un']>0:
            ss+='{} jobs are funny'.format(dict['un'])
        return ss

    def walk_and_build(self,path,qjobs):
        nUI=qjobs.UI_usage()
        os.chdir(path)
        folders=[x[0] for x in os.walk(path)]
        folders.sort()

        for folder in folders:
            os.chdir(folder)
            # a working folder is a folder with job.begin
            if os.path.isfile('job.begin'):
                print('---------------------')
                print(folder)
                qjobs_folder=qjobs.find_base_folder(folder)
                if len(qjobs_folder)==0:
                    #no standing job, either done or not finished
                    if is_finished_from_job_file(folder):
                        fjob=Fjob.create_done_job(folder)
                        print(fjob.message)
                    else:
                        fjob=Fjob.create_not_done_job(folder)
                        print(fjob.message)
                        # Action: resubmit
                        if nUI<2:
                            submit_job(server="UI")
                            nUI=nUI+1
                        else:
                            submit_job(server="all.q")
                        
                        
                elif len(qjobs_folder)==1:
                    #one job, it is good
                    qjob=qjobs_folder[0]
                    status=qjob.status
                    idx=qjob.idx
                    if status=='r':
                        fjob=Fjob.create_run_job(qjob,folder)
                        print(fjob.message)
                    elif status=="qw":
                        fjob=Fjob.create_wait_job(qjob,folder)
                        print(fjob.message)
                    elif status=="Eqw":
                        fjob=Fjob.create_error_job(qjob,folder)
                        print(fjob.message)
                        # Action: kill it and resubmit
                        kill_job(qjob.idx)
                        if nUI<2:
                            submit_job(server="UI")
                            nUI=nUI+1
                        else:
                            submit_job(server="all.q")
                    else:
                        fjob=Fjob.create_unknown_job(qjob,folder)
                        print(fjob.message)
                else:
                    # more than one job, report it
                    fjob=Fjob.create_plural_job(qjobs_folder,folder)
                    print(fjob.message)
                    
                self.fjobs.append(fjob)
                os.chdir(path)

    def info_normal_jobs(self,status):
        # I don't need do anything here
        ss=''
        if status not in ('r','w','d','nd'):
            return ss
        for job in self.fjobs:
            if job.status==status: 
                ss+=job.folder+'\n'
        return ss

    def info_funny_jobs(self):
        # display detailed info here
        ss=''
        for job in self.fjobs:
            if job.status not in ('r','w','d','nd'):
                ss+=str(job)+'\n'
        return ss

############################
# main function
##############################
def main(path):
    # build the queue information
    print(character_frame('My queue information'))
    qjobs=Qjob_list()
    qjobs.myq()
    print(qjobs.short_str())
    print('-'*50)
    print(qjobs)

    nUI=qjobs.UI_usage()
    print("There are {} UI jobs".format(nUI))
    print("{} jobs running; {} jobs submitted\n".format(
        qjobs.n_rjobs(),qjobs.n_jobs()))

    
    # visit all the simulation folders 
    print(character_frame('Walk through simulation folders'))
    fjobs=Fjob_list()
    fjobs.walk_and_build(path,qjobs)
    os.chdir(path)
    print(' ')

    ss=fjobs.info_normal_jobs('r')
    if ss:
        print(character_frame('Running jobs'))
        print(ss)

    ss=fjobs.info_normal_jobs('qw')
    if ss:
        print(character_frame('Waiting jobs'))
        print(ss)

    ss=fjobs.info_normal_jobs('nd')
    if ss:
        print(character_frame('Not done, resubmit jobs'))
        print(ss)
        
    
    ss=fjobs.info_funny_jobs()
    if ss:
        print(character_frame('Funny jobs'))
        print(ss)

    print(character_frame('Summary'))
    print("There are {} UI qjobs".format(nUI))
    print("{} qjobs running; {} qjobs submitted".format(
        qjobs.n_rjobs(),qjobs.n_jobs()))
    print(fjobs.summary())

############################
# run main function
##############################
class common:
    path='./'

def my_job():
    oldstdout = sys.stdout
    sys.stdout = open('currentjob.txt', 'w+')
    main(common.path)
    sys.stdout.flush()
    sys.stdout=oldstdout

common.path=os.getcwd()
scheduler = BlockingScheduler()
scheduler.add_job(my_job, 'interval', minutes=10,
                  next_run_time=datetime.datetime.now())
scheduler.start()






