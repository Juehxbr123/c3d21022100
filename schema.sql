CREATE TABLE IF NOT EXISTS orders (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  username VARCHAR(255) NULL,
  full_name VARCHAR(255) NULL,
  branch VARCHAR(64) NOT NULL,
  status ENUM('draft','new','in_work','done','canceled') NOT NULL DEFAULT 'draft',
  summary TEXT NULL,
  order_payload JSON NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_orders_user_status (user_id, status),
  KEY idx_orders_status_created (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS order_files (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  order_id BIGINT UNSIGNED NOT NULL,
  telegram_file_id VARCHAR(255) NOT NULL,
  telegram_message_id BIGINT NULL,
  original_name VARCHAR(255) NOT NULL,
  mime_type VARCHAR(255) NULL,
  file_size BIGINT NULL,
  local_path VARCHAR(512) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_order_files_order (order_id),
  CONSTRAINT fk_order_files_order FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS order_messages (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  order_id BIGINT UNSIGNED NOT NULL,
  direction ENUM('in','out') NOT NULL,
  message_text TEXT NOT NULL,
  telegram_message_id BIGINT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_order_messages_order (order_id, created_at),
  CONSTRAINT fk_order_messages_order FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bot_config (
  config_key VARCHAR(191) NOT NULL,
  config_value TEXT NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (config_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO bot_config (config_key, config_value)
VALUES
('welcome_menu_msg', 'Добро пожаловать в Chel3D 👋\nВыберите нужный пункт меню:'),
('about_text', 'Chel3D — 3D-печать, 3D-сканирование и помощь в создании модели.')
ON DUPLICATE KEY UPDATE config_value = VALUES(config_value);
