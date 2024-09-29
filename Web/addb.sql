-- phpMyAdmin SQL Dump
-- version 4.8.4
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Feb 19, 2019 at 04:40 PM
-- Server version: 10.1.37-MariaDB
-- PHP Version: 5.6.40

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `addb`
--

-- --------------------------------------------------------

--
-- Table structure for table `accident`
--

CREATE TABLE `accident` (
  `Accident_id` int(11) NOT NULL,
  `Camera_id` varchar(20) NOT NULL,
  `path` varchar(60) NOT NULL,
  `date_time` datetime NOT NULL,
  `timestampAcc` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `accident`
--

INSERT INTO `accident` (`Accident_id`, `Camera_id`, `path`, `date_time`, `timestampAcc`) VALUES
(1, 'CAM001', '../accident/Accident0.jpg', '2019-02-19 10:09:47', 1550551187);

-- --------------------------------------------------------

--
-- Table structure for table `buffer`
--

CREATE TABLE `buffer` (
  `path` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `cctv`
--

CREATE TABLE `cctv` (
  `Camera_id` varchar(20) NOT NULL,
  `Region_id` varchar(20) NOT NULL,
  `latitude` varchar(20) NOT NULL,
  `longitude` varchar(20) NOT NULL,
  `working` int(11) NOT NULL,
  `location` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `cctv`
--

INSERT INTO `cctv` (`Camera_id`, `Region_id`, `latitude`, `longitude`, `working`, `location`) VALUES
('CAM001', 'RAIT001', '19.0330° N', ' 73.0297° E', 0, 'NERUL NAVI MUMBAI');

-- --------------------------------------------------------

--
-- Table structure for table `emergency_service`
--

CREATE TABLE `emergency_service` (
  `EmergencyService_id` varchar(20) NOT NULL,
  `Type` varchar(20) NOT NULL,
  `Name` varchar(50) NOT NULL,
  `Address` varchar(50) NOT NULL,
  `Telephone` varchar(14) NOT NULL,
  `Region_id` varchar(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `emergency_service`
--

INSERT INTO `emergency_service` (`EmergencyService_id`, `Type`, `Name`, `Address`, `Telephone`, `Region_id`) VALUES
('EM001', 'Hospital', 'Nerul', 'DY Patil Hospital', '+91-000000', 'RAIT001'),
('EM002', 'Fire Station', 'Vashi', 'NMMC Fire Station', '+91-111111', 'RAIT001'),
('EM003', 'Police Station', 'Nerul', 'Nerul Police Thane', '+91-222222', 'RAIT001');

-- --------------------------------------------------------

--
-- Table structure for table `flag`
--

CREATE TABLE `flag` (
  `flag_var` int(11) NOT NULL,
  `flag_key` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `flag`
--

INSERT INTO `flag` (`flag_var`, `flag_key`) VALUES
(1, 0);

-- --------------------------------------------------------

--
-- Table structure for table `monitoring_region`
--

CREATE TABLE `monitoring_region` (
  `Region_id` varchar(20) NOT NULL,
  `Address` varchar(50) NOT NULL,
  `Password` varchar(20) NOT NULL,
  `Telephone` varchar(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `monitoring_region`
--

INSERT INTO `monitoring_region` (`Region_id`, `Address`, `Password`, `Telephone`) VALUES
('RAIT001', 'Ramrao Adik Institut', '12345', '+91-1000000000');

-- --------------------------------------------------------

--
-- Table structure for table `smbool`
--

CREATE TABLE `smbool` (
  `flag_var` int(11) NOT NULL,
  `continue_buffer` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `smbool`
--

INSERT INTO `smbool` (`flag_var`, `continue_buffer`) VALUES
(0, 0);

-- --------------------------------------------------------

--
-- Table structure for table `temp`
--

CREATE TABLE `temp` (
  `path` varchar(20) NOT NULL,
  `date_time` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `vidbuffer`
--

CREATE TABLE `vidbuffer` (
  `path` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `vids`
--

CREATE TABLE `vids` (
  `path` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `vids`
--

INSERT INTO `vids` (`path`) VALUES
('../vidbuffer/video.mp4'),
('../vidbuffer/video1.mp4');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `accident`
--
ALTER TABLE `accident`
  ADD PRIMARY KEY (`Accident_id`),
  ADD KEY `Camera_id` (`Camera_id`);

--
-- Indexes for table `buffer`
--
ALTER TABLE `buffer`
  ADD PRIMARY KEY (`path`);

--
-- Indexes for table `cctv`
--
ALTER TABLE `cctv`
  ADD PRIMARY KEY (`Camera_id`),
  ADD KEY `Region_id` (`Region_id`);

--
-- Indexes for table `emergency_service`
--
ALTER TABLE `emergency_service`
  ADD PRIMARY KEY (`EmergencyService_id`),
  ADD KEY `Region_id` (`Region_id`);

--
-- Indexes for table `flag`
--
ALTER TABLE `flag`
  ADD PRIMARY KEY (`flag_key`);

--
-- Indexes for table `monitoring_region`
--
ALTER TABLE `monitoring_region`
  ADD PRIMARY KEY (`Region_id`);

--
-- Indexes for table `smbool`
--
ALTER TABLE `smbool`
  ADD PRIMARY KEY (`flag_var`);

--
-- Indexes for table `vidbuffer`
--
ALTER TABLE `vidbuffer`
  ADD PRIMARY KEY (`path`);

--
-- Indexes for table `vids`
--
ALTER TABLE `vids`
  ADD PRIMARY KEY (`path`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `accident`
--
ALTER TABLE `accident`
  MODIFY `Accident_id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `accident`
--
ALTER TABLE `accident`
  ADD CONSTRAINT `Accident_fk_1` FOREIGN KEY (`Camera_id`) REFERENCES `cctv` (`Camera_id`);

--
-- Constraints for table `cctv`
--
ALTER TABLE `cctv`
  ADD CONSTRAINT `cctv_ibfk_1` FOREIGN KEY (`Region_id`) REFERENCES `monitoring_region` (`Region_id`);

--
-- Constraints for table `emergency_service`
--
ALTER TABLE `emergency_service`
  ADD CONSTRAINT `emergency_service_ibfk_1` FOREIGN KEY (`Region_id`) REFERENCES `monitoring_region` (`Region_id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
