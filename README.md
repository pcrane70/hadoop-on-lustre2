# Diskless Hadoop 2 (YARN) on Lustre

This repository contains the code changes that allows Hadoop2 (YARN) to be run
on "diskless" Hadoop nodes that use Lustre for all storage (temporary
and permanent). This version of Hadoop should be built in a manner
identical to unpatched Hadoop.

Please see [this PDF](http://www.xyratex.com/sites/default/files/Xyratex_white_paper_MapReduce_1-4.pdf)
for a more detailed explanation of how this patch works.
The PDF is also included in the repository.

Here is a list of the bare minimum paramters that need to be changed in
order to run this software in "diskless" mode.

* **Filename** | **Configuration Parameter** | **Suggested value**
* core-site.xml | fs.defaultFS | file:///
* core-site.xml | hadoop.tmp.dir | Any location on the Lustre filesystem
* mapred-site.xml | yarn.app.mapreduce.am.staging-dir | <lustre_mount_point>/tmp/yarn-staging
* mapred-site.xml | mapred.cluster.local.dir | Any local path outside <lustre_mount_point>
* mapred-site.xml | lustre.dir | Any directory on Lustre
* mapred-site.xml | hadoop.ln.cmd | Set to the full path for the system “ln” command
* yarn-site.xml | yarn.nodemanager.local-dirs | Any directory on Lustre named "${host.name}"
* yarn-env.sh | HADOOP\_OPTS | Add "-Dhost.name=`hostname -s`"
* yarn-env.sh | "-XX:ErrorFile" and "-Xloggc" | Modify HADOOP\_NAMENODE\_OPTS,
  HADOOP\_JOBTRACKER\_OPTS, and HADOOP\_SECONDARYNAMENODE\_OPTS to 
  point to non-volatile storage
* yarn-env.sh | HADOOP\_LOG\_DIR and HADOOP\_SECURE\_DN\_LOG\_DIR | Modify the
  paths to point to non-volatile storage


For questions and other inquries, please contact us at
<hadoop.on.lustre@seagate.com>

Seagate Technology
November, 2014