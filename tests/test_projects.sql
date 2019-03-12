-- MySQL dump 10.15  Distrib 10.0.23-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: mariadb    Database: mozilla_pro
-- ------------------------------------------------------
-- Server version	10.0.25-MariaDB-1~jessie

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `project_children`
--

DROP TABLE IF EXISTS `project_children`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `project_children` (
  `project_id` int(11) NOT NULL,
  `subproject_id` int(11) NOT NULL,
  UNIQUE KEY `project_id` (`project_id`,`subproject_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `project_children`
--

LOCK TABLES `project_children` WRITE;
/*!40000 ALTER TABLE `project_children` DISABLE KEYS */;
/*!40000 ALTER TABLE `project_children` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `project_repositories`
--

DROP TABLE IF EXISTS `project_repositories`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `project_repositories` (
  `project_id` int(11) NOT NULL,
  `data_source` varchar(32) NOT NULL,
  `repository_name` varchar(255) NOT NULL,
  UNIQUE KEY `project_id` (`project_id`,`data_source`,`repository_name`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `project_repositories`
--

LOCK TABLES `project_repositories` WRITE;
/*!40000 ALTER TABLE `project_repositories` DISABLE KEYS */;
INSERT INTO `project_repositories` VALUES 
	(1,'its','https://bugzilla.redhat.com --filter-raw=product:Red Hat OpenStack'),
	(2,'its','https://bugzilla.redhat.com --filter-raw=product:ovirt-engine'),
	(1,'its','https://bugzilla.redhat.com --filter-raw=product:Fedora'),
	(2,'its','https://bugzilla.redhat.com --filter-raw=product:OpenShift Container Platform'),
	(1,'its','https://bugzilla.redhat.com --filter-raw=product:Red Hat Enterprise Linux 7'),
	(1,'its','https://bugzilla.redhat.com --filter-raw=product:ovirt-hosted-engine-setup'),
	(2,'its','https://bugzilla.redhat.com --filter-raw=product:Red Hat Gluster Storage');
INSERT INTO `project_repositories` VALUES 
	(1,'bugzillarest','https://bugzilla.mozilla.org --filter-raw=product:Core'),
	(2,'bugzillarest','https://bugzilla.mozilla.org --filter-raw=product:Firefox for Android'),
	(1,'bugzillarest','https://bugzilla.mozilla.org --filter-raw=product:Firefox');
INSERT INTO `project_repositories` VALUES 
	(1,'scr','review.openstack.org_openstack/neutron-specs'),
	(2,'scr','review.openstack.org_openstack/neutron'),
	(1,'scr','review.openstack.org_openstack/ironic'),
	(2,'scr','review.openstack.org_openstack/instack-undercloud'),
	(1,'scr','review.openstack.org_openstack/tripleo-heat-templates');
INSERT INTO `project_repositories` VALUES 
	(1,'scm','https://github.com/grimoirelab/perceval.git');
INSERT INTO `project_repositories` VALUES 
	(1,'mls','/mnt/mailman_archives/http://example.com.mbox/http://example.com.mbox');
/*!40000 ALTER TABLE `project_repositories` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `projects`
--

DROP TABLE IF EXISTS `projects`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `projects` (
  `project_id` int(11) NOT NULL AUTO_INCREMENT,
  `id` varchar(255) NOT NULL,
  `title` varchar(255) NOT NULL,
  PRIMARY KEY (`project_id`)
) ENGINE=MyISAM AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `projects`
--

LOCK TABLES `projects` WRITE;
/*!40000 ALTER TABLE `projects` DISABLE KEYS */;
INSERT INTO `projects` VALUES (1,'test1','Test1'),(2,'test2','Test2');
/*!40000 ALTER TABLE `projects` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-07-28  8:32:42
