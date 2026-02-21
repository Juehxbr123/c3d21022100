<?php
// Author: Sergey Akulov
// GitHub: https://github.com/serg-akulov

// Подключаем конфигурацию, созданную установщиком
if (!file_exists('php_config.php')) {
    die('<div style="font-family:sans-serif;padding:20px;color:#721c24;background-color:#f8d7da;border:1px solid #f5c6cb;border-radius:5px;max-width:600px;margin:20px auto;">
        <strong>❌ Ошибка конфигурации:</strong><br>Файл <code>php_config.php</code> не найден.<br><br>
        Если вы только что установили бота, убедитесь, что запустили <code>install.sh</code>.<br>
        Если вы переносите админку вручную, скопируйте файл конфигурации в эту же папку.
        </div>');
}
require 'php_config.php';

// Старт сессии и авторизация
session_start();
if (isset($_POST['login_pass'])) {
    if ($_POST['login_pass'] === $admin_pass) $_SESSION['auth'] = true;
}

if (isset($_GET['logout'])) { 
    session_destroy(); 
    header("Location: admin.php"); 
    exit; 
}

if (!isset($_SESSION['auth'])) {
    echo '<!DOCTYPE html><html><head><title>Вход</title><meta name="viewport" content="width=device-width, initial-scale=1"><link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet"></head><body class="bg-light d-flex align-items-center justify-content-center" style="height:100vh;"><div class="card p-4 shadow" style="width:100%;max-width:400px;"><h4 class="mb-3 text-center">🔐 Вход в CRM</h4><form method="POST"><input type="password" name="login_pass" class="form-control mb-3" placeholder="Пароль администратора"><button class="btn btn-primary w-100">Войти</button></form></div></body></html>';
    exit;
}

// Подключение к БД
$mysqli = new mysqli($db_host, $db_user, $db_pass, $db_name);
if ($mysqli->connect_error) {
    die("Ошибка подключения к Базе Данных: " . $mysqli->connect_error);
}
$mysqli->set_charset("utf8mb4");

// ПОЛУЧАЕМ ТОКЕН БОТА ИЗ БД
$res_token = $mysqli->query("SELECT value_text FROM settings WHERE key_name = 'bot_token'");
$token_row = $res_token->fetch_assoc();
$BOT_TOKEN = $token_row['value_text'] ?? '';

// --- РЕЖИМ ПРОСМОТРА ФОТО (ОТДЕЛЬНАЯ СТРАНИЦА) ---
if (isset($_GET['view_photos'])) {
    $oid = (int)$_GET['view_photos'];
    $order = $mysqli->query("SELECT * FROM orders WHERE id = $oid")->fetch_assoc();
    
    echo '<!DOCTYPE html><html><head><title>Фото заказа #'.$oid.'</title><link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet"></head><body class="bg-dark text-white p-4">';
    echo '<div class="container">';
    echo '<h3>📸 Фото к заказу #'.$oid.'</h3><a href="admin.php" class="btn btn-outline-light btn-sm mb-3">← Назад</a><hr>';
    
    if ($order && $order['photo_file_id']) {
        $ids = explode(',', $order['photo_file_id']);
        echo '<div class="row">';
        foreach($ids as $file_id) {
            // Запрашиваем путь к файлу у Telegram API
            $json = @file_get_contents("https://api.telegram.org/bot$BOT_TOKEN/getFile?file_id=".trim($file_id));
            $data = json_decode($json, true);
            if (isset($data['result']['file_path'])) {
                $url = "https://api.telegram.org/file/bot$BOT_TOKEN/" . $data['result']['file_path'];
                echo '<div class="col-md-4 mb-4"><div class="card bg-secondary"><a href="'.$url.'" target="_blank"><img src="'.$url.'" class="card-img-top" style="object-fit: contain; height: 300px; background: #222;"></a></div></div>';
            } else {
                echo '<div class="col-md-4"><div class="alert alert-warning">Файл устарел или недоступен (Telegram хранит файлы ограниченное время)</div></div>';
            }
        }
        echo '</div>';
    } else {
        echo 'Нет фото.';
    }
    echo '</div></body></html>';
    exit; // Прерываем скрипт
}

// --- СТАТУСЫ ---
$status_map = [
    'filling' => ['text' => '✍️ Заполняет...', 'class' => 'bg-light text-muted border'],
    'new' => ['text' => '🔥 НОВЫЙ', 'class' => 'bg-success text-white'],
    'discussion' => ['text' => '💬 Обсуждение', 'class' => 'bg-info text-dark'],
    'approved' => ['text' => '🛠 В работе', 'class' => 'bg-primary text-white'],
    'done' => ['text' => '✅ ГОТОВ', 'class' => 'bg-dark text-white'],
    'rejected' => ['text' => '❌ Отказ', 'class' => 'bg-danger text-white']
];

// --- ФУНКЦИЯ ОТПРАВКИ СООБЩЕНИЯ ---
function send_telegram_msg($user_id, $text) {
    global $BOT_TOKEN;
    if (!$BOT_TOKEN || !$user_id) return;
    $url = "https://api.telegram.org/bot$BOT_TOKEN/sendMessage";
    $data = ['chat_id' => $user_id, 'text' => $text, 'parse_mode' => 'HTML'];
    $options = ['http' => ['header' => "Content-type: application/x-www-form-urlencoded\r\n", 'method' => 'POST', 'content' => http_build_query($data), 'ignore_errors' => true]];
    $context  = stream_context_create($options);
    @file_get_contents($url, false, $context);
}

// --- СОХРАНЕНИЕ НАСТРОЕК ---
if (isset($_POST['save_config'])) {
    foreach ($_POST['cfg'] as $key => $value) {
        $clean_val = $mysqli->real_escape_string($value);
        $mysqli->query("INSERT INTO bot_config (cfg_key, cfg_value, description) VALUES ('$key', '$clean_val', '') ON DUPLICATE KEY UPDATE cfg_value='$clean_val'");
    }
    $msg = "✅ Настройки сохранены!";
}

// --- ОБНОВЛЕНИЕ ЗАКАЗА ---
if (isset($_POST['update_order'])) {
    $oid = (int)$_POST['order_id'];
    $new_status = $_POST['status'];
    $note = $mysqli->real_escape_string($_POST['internal_note']);
    
    $order_info = $mysqli->query("SELECT user_id, status FROM orders WHERE id = $oid")->fetch_assoc();
    $old_status = $order_info['status'];
    
    $mysqli->query("UPDATE orders SET status = '$new_status', internal_note = '$note' WHERE id = $oid");
    
    if ($old_status != $new_status) {
        $status_text = $status_map[$new_status]['text'] ?? $new_status;
        $client_msg = "ℹ️ <b>Статус заказа #$oid изменен:</b>\n\n" . $status_text;
        if ($new_status == 'done') $client_msg .= "\n\n🎉 Ваш заказ готов! Можете забирать.";
        send_telegram_msg($order_info['user_id'], $client_msg);
    }
    $msg = "✅ Заказ #$oid обновлен!";
}

// --- ФИЛЬТР И ВЫБОРКА ---
$selected_month = $_GET['m'] ?? date('n');
$selected_year = $_GET['y'] ?? date('Y');
$sql = "SELECT * FROM orders WHERE MONTH(created_at) = $selected_month AND YEAR(created_at) = $selected_year AND status != 'filling' ORDER BY id DESC";
$orders = $mysqli->query($sql);

// ЗАГРУЗКА КОНФИГА БОТА (для вкладки настроек)
$config_result = $mysqli->query("SELECT * FROM bot_config");
$cfg = [];
while($row = $config_result->fetch_assoc()) $cfg[$row['cfg_key']] = $row['cfg_value'];

// Хелперы для вывода полей
function render_input($key, $label, $rows=2) { 
    global $cfg; 
    $val = htmlspecialchars($cfg[$key] ?? ''); 
    echo "<div class='mb-2'><label class='form-label small text-muted fw-bold text-uppercase'>$label</label><textarea name='cfg[$key]' class='form-control' rows='$rows'>$val</textarea></div>"; 
}
function render_switch($key, $label) { 
    global $cfg; 
    $val = $cfg[$key] ?? '0'; 
    $checked = ($val == '1') ? 'checked' : ''; 
    echo "<div class='form-check form-switch mb-3'><input type='hidden' name='cfg[$key]' value='0'><input class='form-check-input' type='checkbox' name='cfg[$key]' value='1' id='$key' $checked><label class='form-check-label fw-bold' for='$key'>$label</label></div>"; 
}
?>
<!DOCTYPE html>
<html>
<head>
    <title>Metalok CRM</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        .step-card { border-left: 5px solid #0d6efd; margin-bottom: 20px; }
        .step-header { background: #f8f9fa; padding: 10px; font-weight: bold; border-bottom: 1px solid #eee; }
        .order-row { cursor: pointer; transition: 0.2s; }
        .order-row:hover { background-color: #f1f1f1; }
        .filter-bar { background: #e9ecef; padding: 10px; border-radius: 8px; margin-bottom: 20px; }
    </style>
</head>
<body class="bg-light pb-5">

<nav class="navbar navbar-dark bg-dark sticky-top mb-4">
    <div class="container">
        <span class="navbar-brand mb-0 h1">🛠 Metalok CRM</span>
        <a href="?logout=1" class="btn btn-outline-danger btn-sm">Выход</a>
    </div>
</nav>

<div class="container">
    <?php if(isset($msg)) echo "<div class='alert alert-success'>$msg</div>"; ?>

    <ul class="nav nav-pills mb-4" id="pills-tab" role="tablist">
        <li class="nav-item"><button class="nav-link active" data-bs-toggle="pill" data-bs-target="#orders">📋 Заказы</button></li>
        <li class="nav-item"><button class="nav-link" data-bs-toggle="pill" data-bs-target="#settings">⚙️ Конструктор</button></li>
    </ul>

    <div class="tab-content">
        <!-- Вкладка: ЗАКАЗЫ -->
        <div class="tab-pane fade show active" id="orders">
            <form method="GET" class="filter-bar d-flex align-items-center gap-2 shadow-sm">
                <span class="fw-bold text-muted">📅 Архив:</span>
                <select name="m" class="form-select form-select-sm" style="width:auto;">
                    <?php 
                    $months = [1=>'Январь',2=>'Февраль',3=>'Март',4=>'Апрель',5=>'Май',6=>'Июнь',7=>'Июль',8=>'Август',9=>'Сентябрь',10=>'Октябрь',11=>'Ноябрь',12=>'Декабрь'];
                    foreach($months as $n => $name) {
                        $sel = ($n == $selected_month) ? 'selected' : '';
                        echo "<option value='$n' $sel>$name</option>";
                    }
                    ?>
                </select>
                <select name="y" class="form-select form-select-sm" style="width:auto;">
                    <?php 
                    for($y=2024; $y<=2030; $y++) {
                        $sel = ($y == $selected_year) ? 'selected' : '';
                        echo "<option value='$y' $sel>$y</option>";
                    }
                    ?>
                </select>
                <button type="submit" class="btn btn-sm btn-primary">Показать</button>
            </form>

            <div class="table-responsive bg-white shadow rounded p-3">
                <table class="table table-hover align-middle">
                    <thead class="table-light">
                        <tr><th>ID</th><th>Дата</th><th>Статус</th><th>Клиент</th><th>Задача</th><th></th></tr>
                    </thead>
                    <tbody>
                        <?php 
                        if ($orders->num_rows == 0) {
                            echo "<tr><td colspan='6' class='text-center text-muted p-5'>📭 В этом месяце заказов нет</td></tr>";
                        }
                        while($row = $orders->fetch_assoc()): 
                            $st_key = $row['status'] ?? 'new';
                            $st_data = $status_map[$st_key] ?? ['text' => $st_key, 'class' => 'bg-secondary'];
                            $date = date('d.m H:i', strtotime($row['created_at']));
                        ?>
                        <tr class="order-row">
                            <td>#<?php echo $row['id']; ?></td>
                            <td><small class="text-muted"><?php echo $date; ?></small></td>
                            <td><span class="badge <?php echo $st_data['class']; ?>"><?php echo $st_data['text']; ?></span></td>
                            <td>
                                <b><?php echo htmlspecialchars($row['full_name'] ?? 'Аноним'); ?></b><br>
                                <a href="https://t.me/<?php echo $row['username'] ?? ''; ?>" target="_blank" class="text-decoration-none small">@<?php echo htmlspecialchars($row['username'] ?? '-'); ?></a>
                            </td>
                            <td>
                                <small class="fw-bold"><?php echo htmlspecialchars($row['work_type'] ?? '-'); ?></small><br>
                                <small class="text-muted"><?php echo mb_strimwidth(htmlspecialchars($row['dimensions_info'] ?? ''), 0, 40, "..."); ?></small>
                            </td>
                            <td class="text-end">
                                <button class="btn btn-sm btn-outline-secondary" 
                                    data-bs-toggle="modal" 
                                    data-bs-target="#orderModal" 
                                    data-id="<?php echo $row['id']; ?>"
                                    data-fullname="<?php echo htmlspecialchars($row['full_name'] ?? ''); ?>"
                                    data-type="<?php echo htmlspecialchars($row['work_type'] ?? ''); ?>"
                                    data-dims="<?php echo htmlspecialchars($row['dimensions_info'] ?? ''); ?>"
                                    data-conds="<?php echo htmlspecialchars($row['conditions'] ?? ''); ?>"
                                    data-urgency="<?php echo htmlspecialchars($row['urgency'] ?? ''); ?>"
                                    data-comment="<?php echo htmlspecialchars($row['comment'] ?? ''); ?>"
                                    data-note="<?php echo htmlspecialchars($row['internal_note'] ?? ''); ?>"
                                    data-status="<?php echo $row['status'] ?? 'new'; ?>"
                                    data-photo="<?php echo $row['photo_file_id'] ? $row['photo_file_id'] : ''; ?>"
                                >👁</button>
                            </td>
                        </tr>
                        <?php endwhile; ?>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Вкладка: НАСТРОЙКИ -->
        <div class="tab-pane fade" id="settings">
            <form method="POST">
                <div class="card shadow-sm step-card mb-3" style="border-left-color: #198754;"><div class="step-header">👋 Начало работы</div><div class="card-body"><?php render_input('welcome_msg', 'Текст на /start'); ?></div></div>
                <div class="card shadow-sm step-card mb-3"><div class="step-header">📸 Шаг 1: Фото</div><div class="card-body"><?php render_input('step_photo_text', 'Вопрос'); ?><div class="row mt-2"><div class="col-md-8"><?php render_input('btn_skip_photo', 'Кнопка "Пропустить"'); ?></div><div class="col-md-4 pt-4"><?php render_switch('is_photo_required', 'ФОТО ОБЯЗАТЕЛЬНО?'); ?></div></div></div></div>
                <div class="card shadow-sm step-card mb-3"><div class="step-header">🛠 Шаг 2: Тип работы</div><div class="card-body"><?php render_input('step_type_text', 'Вопрос'); ?><div class="btn-input-group"><h6 class="text-muted">Кнопки:</h6><div class="row"><div class="col-4"><?php render_input('btn_type_repair', 'Кнопка 1'); ?></div><div class="col-4"><?php render_input('btn_type_copy', 'Кнопка 2'); ?></div><div class="col-4"><?php render_input('btn_type_drawing', 'Кнопка 3'); ?></div></div></div></div></div>
                <div class="card shadow-sm step-card mb-3"><div class="step-header">📏 Шаг 3: Размеры</div><div class="card-body"><?php render_input('step_dim_text', 'Вопрос'); ?></div></div>
                <div class="card shadow-sm step-card mb-3"><div class="step-header">⚙️ Шаг 4: Условия</div><div class="card-body"><?php render_input('step_cond_text', 'Вопрос'); ?><div class="btn-input-group"><h6 class="text-muted">Кнопки:</h6><div class="row"><div class="col-6 mb-2"><?php render_input('btn_cond_rotation', 'Кнопка 1'); ?></div><div class="col-6 mb-2"><?php render_input('btn_cond_static', 'Кнопка 2'); ?></div><div class="col-6"><?php render_input('btn_cond_impact', 'Кнопка 3'); ?></div><div class="col-6"><?php render_input('btn_cond_unknown', 'Кнопка 4'); ?></div></div></div></div></div>
                <div class="card shadow-sm step-card mb-3"><div class="step-header">⏳ Шаг 5: Срочность</div><div class="card-body"><?php render_input('step_urgency_text', 'Вопрос'); ?><div class="btn-input-group"><h6 class="text-muted">Кнопки:</h6><div class="row"><div class="col-4"><?php render_input('btn_urgency_high', 'Кнопка 1'); ?></div><div class="col-4"><?php render_input('btn_urgency_med', 'Кнопка 2'); ?></div><div class="col-4"><?php render_input('btn_urgency_low', 'Кнопка 3'); ?></div></div></div></div></div>
                <div class="card shadow-sm step-card mb-3" style="border-left-color: #fd7e14;"><div class="step-header">➕ Дополнительный вопрос</div><div class="card-body"><?php render_switch('step_extra_enabled', 'ВКЛЮЧИТЬ?'); ?><?php render_input('step_extra_text', 'Текст вопроса'); ?></div></div>
                <div class="card shadow-sm step-card mb-3" style="border-left-color: #198754;"><div class="step-header">🏁 Финал</div><div class="card-body"><?php render_input('step_final_text', 'Вопрос'); ?><?php render_input('msg_done', 'Успех'); ?></div></div>
                
                <div class="sticky-bottom bg-white p-3 shadow-lg border-top">
                    <div class="row align-items-center">
                        <div class="col-md-8"><p class="mb-0 text-muted small">ℹ️ Тексты обновляются сразу.<br>🆘 Рестарт: <code class="bg-dark text-white p-1 rounded">systemctl restart turner_bot</code></p></div>
                        <div class="col-md-4 text-end"><button type="submit" name="save_config" class="btn btn-success btn-lg px-5">💾 СОХРАНИТЬ ВСЁ</button></div>
                    </div>
                </div>
            </form>
        </div>
    </div>
</div>

<!-- МОДАЛКА ПРОСМОТРА ЗАКАЗА -->
<div class="modal fade" id="orderModal" tabindex="-1">
    <div class="modal-dialog modal-lg">
        <form method="POST" class="modal-content">
            <div class="modal-header"><h5 class="modal-title">Заказ #<span id="m_id"></span></h5><button type="button" class="btn-close" data-bs-dismiss="modal"></button></div>
            <div class="modal-body">
                <input type="hidden" name="order_id" id="input_id"><input type="hidden" name="update_order" value="1">
                <div class="row">
                    <div class="col-md-6"><p><strong>👤 Клиент:</strong> <span id="m_client"></span></p><p><strong>🛠 Тип:</strong> <span id="m_type"></span></p><p><strong>📏 Размеры:</strong> <br><span id="m_dims" class="text-primary"></span></p></div>
                    <div class="col-md-6">
                        <p><strong>⚙️ Условия:</strong> <span id="m_conds"></span></p><p><strong>⏳ Срочность:</strong> <span id="m_urgency"></span></p>
                        <p><strong>📸 Фото:</strong> <span id="m_photo_status"></span></p>
                        <!-- КНОПКА ФОТО -->
                        <div id="btn_photo_container" class="d-none">
                            <a href="#" id="link_photos" target="_blank" class="btn btn-warning btn-sm w-100">📂 Смотреть фото</a>
                        </div>
                    </div>
                </div>
                <div class="alert alert-secondary mt-2"><strong>📝 Комментарий:</strong><br><span id="m_comment"></span></div>
                <hr><h6>🔧 Управление</h6>
                <div class="row g-3">
                    <div class="col-md-6"><label class="form-label">Статус:</label><select name="status" id="select_status" class="form-select"><option value="new">🔥 Новый</option><option value="discussion">💬 Обсуждение</option><option value="approved">🛠 В работе</option><option value="done">✅ Готов</option><option value="rejected">❌ Отказ</option></select></div>
                    <div class="col-md-6"><label class="form-label">Заметка:</label><textarea name="internal_note" id="m_note" class="form-control" rows="2"></textarea></div>
                </div>
            </div>
            <div class="modal-footer"><button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Закрыть</button><button type="submit" class="btn btn-primary">Сохранить</button></div>
        </form>
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
<script>
    const modal = document.getElementById('orderModal');
    modal.addEventListener('show.bs.modal', event => {
        const btn = event.relatedTarget;
        const safe = (t) => t ? t : '-';
        document.getElementById('m_id').innerText = btn.dataset.id;
        document.getElementById('input_id').value = btn.dataset.id;
        document.getElementById('m_client').innerText = safe(btn.dataset.fullname);
        document.getElementById('m_type').innerText = safe(btn.dataset.type);
        document.getElementById('m_dims').innerText = safe(btn.dataset.dims);
        document.getElementById('m_conds').innerText = safe(btn.dataset.conds);
        document.getElementById('m_urgency').innerText = safe(btn.dataset.urgency);
        document.getElementById('m_comment').innerText = safe(btn.dataset.comment);
        document.getElementById('m_note').value = btn.dataset.note;
        document.getElementById('select_status').value = btn.dataset.status;
        
        // Логика кнопки фото
        const photoIds = btn.dataset.photo;
        const photoContainer = document.getElementById('btn_photo_container');
        const photoStatus = document.getElementById('m_photo_status');
        const photoLink = document.getElementById('link_photos');
        
        if (photoIds && photoIds.length > 5) {
            photoStatus.innerText = 'Есть';
            photoStatus.className = 'text-success fw-bold';
            photoContainer.classList.remove('d-none');
            photoLink.href = 'admin.php?view_photos=' + btn.dataset.id;
        } else {
            photoStatus.innerText = 'Нет';
            photoStatus.className = 'text-muted';
            photoContainer.classList.add('d-none');
        }
    });
</script>
</body>
</html>