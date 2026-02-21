<?php
session_start();

$adminUser = getenv('ADMIN_USER') ?: 'admin';
$adminPassword = getenv('ADMIN_PASSWORD') ?: 'change_me';
$botToken = getenv('BOT_TOKEN') ?: '';

$pdo = null;
$error = null;

try {
    $host = getenv('MYSQL_HOST') ?: 'mysql';
    $port = getenv('MYSQL_PORT') ?: '3306';
    $db = getenv('MYSQL_DB') ?: 'chel3d_db';
    $user = getenv('MYSQL_USER') ?: 'chel3d_user';
    $pass = getenv('MYSQL_PASSWORD') ?: '';

    $dsn = "mysql:host={$host};port={$port};dbname={$db};charset=utf8mb4";
    $pdo = new PDO($dsn, $user, $pass, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]);
} catch (Throwable $e) {
    $error = $e->getMessage();
}

if (isset($_GET['logout'])) {
    session_destroy();
    header('Location: /');
    exit;
}

if (!isset($_SESSION['logged_in'])) {
    if ($_SERVER['REQUEST_METHOD'] === 'POST') {
        $u = $_POST['username'] ?? '';
        $p = $_POST['password'] ?? '';
        if ($u === $adminUser && $p === $adminPassword) {
            $_SESSION['logged_in'] = true;
            header('Location: /');
            exit;
        }
        $error = 'Неверный логин или пароль';
    }

    echo '<!doctype html><html lang="ru"><head><meta charset="utf-8"><title>Chel3D Admin Login</title><style>body{font-family:Arial;background:#f5f5f5}form{max-width:360px;margin:80px auto;padding:20px;background:#fff;border-radius:8px}input{width:100%;margin:8px 0;padding:10px}button{padding:10px 14px}.err{color:#b00}</style></head><body>';
    echo '<form method="post"><h2>Вход в админку</h2>';
    if ($error) echo '<p class="err">'.htmlspecialchars($error).'</p>';
    echo '<input name="username" placeholder="Логин" required>';
    echo '<input type="password" name="password" placeholder="Пароль" required>';
    echo '<button type="submit">Войти</button></form></body></html>';
    exit;
}

if ($error) {
    echo "<p>Ошибка БД: " . htmlspecialchars($error) . "</p>";
    exit;
}

$statuses = ['new', 'filling', 'submitted', 'in_work', 'done', 'canceled'];

if (isset($_POST['save_config'])) {
    $stmt = $pdo->prepare("INSERT INTO bot_config (config_key, config_value) VALUES (:k,:v) ON DUPLICATE KEY UPDATE config_value=VALUES(config_value)");
    $stmt->execute([':k' => 'welcome_menu_msg', ':v' => $_POST['welcome_menu_msg'] ?? '']);
    $stmt->execute([':k' => 'about_text', ':v' => $_POST['about_text'] ?? '']);
}

if (isset($_POST['set_status'])) {
    $status = $_POST['status'] ?? '';
    $orderId = (int)($_POST['order_id'] ?? 0);
    if (in_array($status, $statuses, true) && $orderId > 0) {
        $stmt = $pdo->prepare('UPDATE orders SET status=:status WHERE id=:id');
        $stmt->execute([':status' => $status, ':id' => $orderId]);
    }
}

$tab = $_GET['tab'] ?? 'dashboard';
$orderId = isset($_GET['order_id']) ? (int)$_GET['order_id'] : 0;

function h($value) { return htmlspecialchars((string)$value, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8'); }

echo '<!doctype html><html lang="ru"><head><meta charset="utf-8"><title>Chel3D Admin</title><style>body{font-family:Arial;margin:20px}nav a{margin-right:14px}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ddd;padding:8px;vertical-align:top}.card{padding:12px;border:1px solid #ddd;margin:8px 0}.muted{color:#666}</style></head><body>';
echo '<h1>Chel3D Admin</h1>';
echo '<p class="muted">Логин: '.h($adminUser).' | <a href="?logout=1">Выйти</a></p>';
echo '<nav><a href="?tab=dashboard">Dashboard</a><a href="?tab=orders">Заказы</a><a href="?tab=config">Настройки бота</a></nav><hr>';

if ($tab === 'dashboard') {
    $totals = [];
    foreach ($statuses as $st) {
        $stmt = $pdo->prepare('SELECT COUNT(*) cnt FROM orders WHERE status=:st');
        $stmt->execute([':st' => $st]);
        $totals[$st] = (int)$stmt->fetch()['cnt'];
    }
    echo '<h2>Dashboard</h2>';
    foreach ($totals as $st => $cnt) {
        echo '<div class="card"><strong>'.h($st).'</strong>: '.h($cnt).'</div>';
    }
}

if ($tab === 'orders') {
    if ($orderId > 0) {
        $stmt = $pdo->prepare('SELECT * FROM orders WHERE id=:id');
        $stmt->execute([':id' => $orderId]);
        $order = $stmt->fetch();
        if (!$order) {
            echo '<p>Заказ не найден</p>';
        } else {
            echo '<h2>Заказ #'.h($order['id']).'</h2>';
            echo '<form method="post"><input type="hidden" name="order_id" value="'.h($order['id']).'">';
            echo '<select name="status">';
            foreach ($statuses as $st) {
                $sel = $st === $order['status'] ? 'selected' : '';
                echo '<option value="'.h($st).'" '.$sel.'>'.h($st).'</option>';
            }
            echo '</select> <button name="set_status" value="1">Сменить статус</button></form>';

            echo '<table>';
            foreach ($order as $k => $v) {
                echo '<tr><th>'.h($k).'</th><td>'.nl2br(h($v)).'</td></tr>';
            }
            echo '</table>';

            $files = $pdo->prepare('SELECT * FROM order_files WHERE order_id=:id ORDER BY created_at ASC');
            $files->execute([':id' => $orderId]);
            $fileRows = $files->fetchAll();
            echo '<h3>Файлы</h3>';
            if (!$fileRows) {
                echo '<p>Нет файлов</p>';
            } else {
                echo '<ul>';
                foreach ($fileRows as $f) {
                    $link = 'download.php?id='.(int)$f['id'];
                    echo '<li>'.h($f['original_name']).' ('.h($f['telegram_file_id']).') - <a href="'.h($link).'">Скачать</a></li>';
                }
                echo '</ul>';
            }
            echo '<p><a href="?tab=orders">← Назад к списку</a></p>';
        }
    } else {
        $statusFilter = $_GET['status'] ?? '';
        if ($statusFilter && in_array($statusFilter, $statuses, true)) {
            $stmt = $pdo->prepare('SELECT * FROM orders WHERE status=:st ORDER BY created_at DESC LIMIT 500');
            $stmt->execute([':st' => $statusFilter]);
        } else {
            $stmt = $pdo->query('SELECT * FROM orders ORDER BY created_at DESC LIMIT 500');
        }
        $orders = $stmt->fetchAll();
        echo '<h2>Заказы</h2>';
        echo '<p>Фильтр: ';
        echo '<a href="?tab=orders">все</a> ';
        foreach ($statuses as $st) {
            echo '<a href="?tab=orders&status='.h($st).'">'.h($st).'</a> ';
        }
        echo '</p>';
        echo '<table><tr><th>ID</th><th>Пользователь</th><th>Ветка</th><th>Статус</th><th>Создан</th><th></th></tr>';
        foreach ($orders as $o) {
            echo '<tr>';
            echo '<td>'.h($o['id']).'</td>';
            echo '<td>'.h($o['full_name']).' @'.h($o['username']).'</td>';
            echo '<td>'.h($o['branch']).'</td>';
            echo '<td>'.h($o['status']).'</td>';
            echo '<td>'.h($o['created_at']).'</td>';
            echo '<td><a href="?tab=orders&order_id='.(int)$o['id'].'">Открыть</a></td>';
            echo '</tr>';
        }
        echo '</table>';
    }
}

if ($tab === 'config') {
    $cfgRows = $pdo->query('SELECT config_key, config_value FROM bot_config')->fetchAll();
    $cfg = [];
    foreach ($cfgRows as $r) $cfg[$r['config_key']] = $r['config_value'];
    echo '<h2>Настройки бота</h2>';
    echo '<form method="post">';
    echo '<label>Приветствие</label><br><textarea name="welcome_menu_msg" rows="4" style="width:100%">'.h($cfg['welcome_menu_msg'] ?? '').'</textarea><br><br>';
    echo '<label>О нас</label><br><textarea name="about_text" rows="4" style="width:100%">'.h($cfg['about_text'] ?? '').'</textarea><br><br>';
    echo '<button name="save_config" value="1">Сохранить</button></form>';
    echo '<p class="muted">Download endpoint uses BOT_TOKEN '.($botToken ? 'configured' : 'missing').'</p>';
}

echo '</body></html>';
