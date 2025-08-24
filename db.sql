CREATE TABLE `xcam_actions` (
  `id` int NOT NULL primary key AUTO_INCREMENT,
  `command` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `additions` text,
  `status` varchar(100) NOT NULL DEFAULT 'pending',
  `updated_at` datetime DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `xcam_cameras` (
  `id` int NOT NULL primary key AUTO_INCREMENT,
  `ip_address` varchar(50) NOT NULL,
  `cam_name` varchar(255) DEFAULT NULL,
  `mac_address` varchar(255) DEFAULT NULL,
  `ip_type` varchar(50) DEFAULT NULL,
  `username` varchar(255) DEFAULT NULL,
  `password` varchar(255) DEFAULT NULL,
  `channel` int NOT NULL DEFAULT '1',
  `status` VARCHAR(50) NULL DEFAULT 'inactive',
  `updated_at` datetime DEFAULT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;