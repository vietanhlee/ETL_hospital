-- MySQL dump 10.13  Distrib 8.0.43, for Win64 (x86_64)
--
-- Host: localhost    Database: hospital_source
-- ------------------------------------------------------
-- Server version	9.4.0

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `departments`
--

DROP TABLE IF EXISTS `departments`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `departments` (
  `department_id` bigint NOT NULL AUTO_INCREMENT,
  `department_code` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `department_name_raw` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `department_group_raw` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `floor` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `building` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `status` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT 'ACTIVE',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`department_id`),
  KEY `idx_department_code` (`department_code`),
  KEY `idx_department_updated_at` (`updated_at`)
) ENGINE=InnoDB AUTO_INCREMENT=516 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `departments`
--

LOCK TABLES `departments` WRITE;
/*!40000 ALTER TABLE `departments` DISABLE KEYS */;
INSERT INTO `departments` VALUES (1,'KHOA_TIM','Khoa Tim mạch','Kham benh','1','B','ACTIVE','2026-05-01 06:00:00','2026-05-12 06:00:00'),(2,'KHOA_NHI','Khoa Nhi','Khám bệnh','2','C','ACTIVE','2026-05-01 06:00:00','2026-05-12 06:00:00'),(3,'KHOA_CLS','CHẨN ĐOÁN HÌNH ẢNH','CLS','5','C','ACTIVE','2026-05-01 06:00:00','2026-05-12 06:00:00'),(4,'KHOA_XN','Khoa Xét nghiệm','CLS','6','B','ACTIVE','2026-05-01 06:00:00','2026-05-12 06:00:00'),(5,'KHOA_NOI','Khoa Nội tổng quát','Khám bệnh','2','A','ACTIVE','2026-05-01 06:00:00','2026-05-12 06:00:00'),(6,'KHOA_NGOAI','KHOA NGOẠI','Khám bệnh','4','C','ACTIVE','2026-05-01 06:00:00','2026-05-12 06:00:00'),(7,'KHOA_SAN','Khoa Sản','Kham benh','6','C','ACTIVE','2026-05-01 06:00:00','2026-05-12 06:00:00'),(8,'KHOA_DA','Khoa Da liễu','Khám bệnh','1','C','ACTIVE','2026-05-01 06:00:00','2026-05-12 06:00:00'),(9,'KHOA_TM','Khoa Tai mũi họng','Khám bệnh','2','B','ACTIVE','2026-05-01 06:00:00','2026-05-12 06:00:00'),(10,'KHOA_CAPCUU','KHOA CẤP CỨU','Kham benh','2','B','ACTIVE','2026-05-01 06:00:00','2026-05-12 06:00:00'),(11,'KHOA_TIM','Khoa tim mach','Kham benh','1','B','ACTIVE','2026-05-01 06:00:00','2026-05-12 06:00:00'),(12,'KHOA_OLD','Khoa cũ','Khám bệnh','7','D','INACTIVE','2026-04-01 06:00:00','2026-05-12 06:00:00'),(13,'KHOA_101','Khoa Hô hấp 101','kham benh','3','C','ACTIVE','2026-05-13 17:53:00','2026-05-14 13:00:00'),(14,'KHOA_102','Khoa Thần kinh 102','Cận lâm sàng','3','A','ACTIVE','2026-05-13 13:04:00','2026-05-14 06:19:00'),(15,'KHOA_103','Khoa Hô hấp 103','kham benh','4','C','ACTIVE','2026-05-13 07:34:00','2026-05-14 13:21:00'),(16,'KHOA_104','Khoa Nội tiết 104','Khám bệnh','8','C','ACTIVE','2026-05-13 08:04:00','2026-05-14 11:40:00'),(17,'KHOA_105','Khoa Răng Hàm Mặt 105','kham benh','6','C','ACTIVE','2026-05-13 06:59:00','2026-05-14 09:17:00'),(18,'KHOA_106','Khoa Thần kinh 106','Cận lâm sàng','5','B','ACTIVE','2026-05-13 15:49:00','2026-05-14 12:59:00'),(19,'KHOA_107','Khoa Thần kinh 107','can lam sang','4','A','ACTIVE','2026-05-13 15:09:00','2026-05-14 07:46:00'),(20,'KHOA_108','Khoa Cơ xương khớp 108','CLS','3','B','ACTIVE','2026-05-13 15:06:00','2026-05-14 08:51:00'),(21,'KHOA_109','Khoa Răng Hàm Mặt 109','can lam sang','7','B','ACTIVE','2026-05-13 15:49:00','2026-05-14 15:38:00'),(22,'KHOA_110','Khoa Thần kinh 110','can lam sang','8','A','ACTIVE','2026-05-13 08:00:00','2026-05-14 18:14:00'),(23,'KHOA_111','Khoa Cơ xương khớp 111','Khám bệnh','3','A','ACTIVE','2026-05-13 08:30:00','2026-05-14 06:20:00'),(24,'KHOA_112','Khoa Nội tiết 112','Khám bệnh','3','C','ACTIVE','2026-05-13 18:28:00','2026-05-14 12:42:00'),(25,'KHOA_113','Khoa Mắt 113','CLS','5','C','ACTIVE','2026-05-13 18:07:00','2026-05-14 09:35:00'),(26,'KHOA_114','Khoa Mắt 114','Cận lâm sàng','7','C','ACTIVE','2026-05-13 13:22:00','2026-05-14 10:53:00'),(27,'KHOA_115','Khoa Răng Hàm Mặt 115','CLS','2','C','ACTIVE','2026-05-13 09:10:00','2026-05-14 16:53:00'),(28,'KHOA_116','Khoa Thần kinh 116','kham benh','7','C','ACTIVE','2026-05-13 13:31:00','2026-05-14 17:50:00'),(29,'KHOA_117','Khoa Thần kinh 117','Khám bệnh','7','C','ACTIVE','2026-05-13 11:11:00','2026-05-14 06:55:00'),(30,'KHOA_118','Khoa Thần kinh 118','Cận lâm sàng','6','C','ACTIVE','2026-05-13 12:13:00','2026-05-14 15:35:00'),(31,'KHOA_119','Khoa Cơ xương khớp 119','Cận lâm sàng','6','A','ACTIVE','2026-05-13 17:00:00','2026-05-14 09:28:00'),(32,'KHOA_120','Khoa Thần kinh 120','can lam sang','4','A','ACTIVE','2026-05-13 15:19:00','2026-05-14 11:06:00'),(501,'KHOA_501','khoa ung bướu 501','điều trị nội trú','7','A','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(502,'KHOA_502','Khoa Nội tiết 502','Khám bệnh','9','A','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(503,'KHOA_503','KHOA LÃO KHOA 503','khám bệnh','9','B','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(504,'KHOA_504','Khoa Chấn thương chỉnh hình 504','phẫu thuật','1','A','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(505,'KHOA_505','KHOA HUYẾT HỌC 505','cận lâm sàng','7','B','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(506,'KHOA_506','KHOA THẬN TIẾT NIỆU 506','Điều trị nội trú','1','C','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(507,'KHOA_507','KHOA DA LIỄU 507','khám bệnh','1','Khu điều trị','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(508,'KHOA_508','KHOA DINH DƯỠNG 508','Khám bệnh','4','Khu khám','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(509,'KHOA_509','Khoa Phục hồi chức năng 509','điều trị nội trú','3','Khu điều trị','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(510,'KHOA_510','KHOA NỘI SOI 510','cận lâm sàng','7','Khu điều trị','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(511,'KHOA_511','khoa hồi sức tích cực 511','HỒI SỨC','5','B','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(512,'KHOA_512','khoa cấp cứu tổng hợp 512','Cấp cứu','9','Khu điều trị','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(513,'KHOA_513','Khoa Cơ xương khớp 513','Khám bệnh','7','B','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(514,'KHOA_514','khoa tâm thần 514','Khám bệnh','8','Khu điều trị','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00'),(515,'KHOA_515','khoa tai mũi họng 515','Khám bệnh','7','C','ACTIVE','2026-06-01 00:00:00','2026-06-01 00:00:00');
/*!40000 ALTER TABLE `departments` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-05-24 14:31:24
