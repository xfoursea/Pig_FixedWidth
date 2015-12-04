# Pig_FixedWidth

This is an add-on to Lester's blog, "how do i load a fixed-width formatted file into hive? (with a little help from pig)".
https://martin.atlassian.net/wiki/pages/viewpage.action?pageId=21299205

Same sample data is used; the following works with Hortonworks Sandbox 2.3.0

It is a Pig driven approach, covering 
   1. Load in FixedWidth file
   2. Output data into hive table
   3. Output data to another FixedWidth file (after process)

Pig Script:

    REGISTER /usr/hdp/2.3.0.0-2557/pig/lib/piggybank.jar;
    REGISTER /usr/hdp/2.3.0.0-2557/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;
    REGISTER /usr/hdp/2.3.0.0-2557/hive-hcatalog/share/hcatalog/hive-hcatalog-pig-adapter.jar;
    
    /* loading in raw data */
    
    emp = LOAD '/user/root/emps-fixed.txt'
        USING org.apache.pig.piggybank.storage.FixedWidthLoader(
         '1-10, 11-30, 31-50, 51-70, 71-79', 'SKIP_HEADER',
        'emp_id: chararray, first_name: chararray, last_name: chararray, job_title: chararray, mgr_emp_id: chararray'
      );
      
    /* 
    data process, skipped
    */   
      
    /* Output 1: hive table */
    
    STORE emp INTO 'emp_table' USING org.apache.hive.hcatalog.pig.HCatStorer();
    
    /* Output 2: FixedWidth file to be scp to external host */
    
    store emp into '/user/root/emps-str' USING org.apache.pig.piggybank.storage.FixedWidthStorer('1-10, 11-30, 31-50, 51-70,   71-79','NO_HEADER');

Notes:

1. default.emp_table should be created beforehand:
    
    hive> create table default.emp_table(emp_id string, first_name string, last_name string, job_title string, mgr_emp_id string);
    
2. FixedWidthStorer, the last column must have a defined end though; "71-" will cause misleading "java heap space" error--- even you increase map reduce heap size, it causes map reduce job hanging.

After Output1, you should be able to see:

    hive> select * from emp_table;
    OK
    EMP-ID	FIRST-NAME	LASTNAME	JOB-TITLE	MGR-EMP-I
    12301	Johnny	Begood	Programmer	12306
    12302	Ainta	Listening	Programmer	12306
    12303	Neva	Mind	Architect	12306
    12304	Joseph	Blow	Tester	12308
    12305	Sallie	Mae	Programmer	12306
    12306	Bilbo	Baggins	Development Manager	12307
    12307	Nuther	One	Director	11111
    12308	Yeta	Notherone	Testing Manager	12307
    12309	Evenmore	Dumbnames	Senior Architect	12307
    12310	Last	Sillyname	Senior Tester	12308
    Time taken: 1.176 seconds, Fetched: 11 row(s)
    
