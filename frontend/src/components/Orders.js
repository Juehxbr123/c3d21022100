import React, { useCallback, useEffect, useState } from 'react';
import {
  Badge,
  Button,
  Card,
  Col,
  Form,
  Image,
  Input,
  message,
  Modal,
  Row,
  Select,
  Space,
  Statistic,
  Table,
  Tag,
} from 'antd';import { ShoppingCartOutlined, SyncOutlined, UserOutlined } from '@ant-design/icons';
import axios from 'axios';
import dayjs from 'dayjs';

const { Option } = Select;

const statusOptions = [
  { value: 'draft', label: 'Черновик' },
  { value: 'new', label: 'Новая заявка' },
  { value: 'in_work', label: 'В работе' },
  { value: 'done', label: 'Готово' },
  { value: 'canceled', label: 'Отменено' },
];

const statusColor = {
  draft: 'default',
  new: 'blue',
  in_work: 'orange',
  done: 'green',
  canceled: 'red',
};

const Orders = () => {
  const [orders, setOrders] = useState([]);
  const [stats, setStats] = useState({ total_orders: 0, new_orders: 0, active_orders: 0 });
  const [loading, setLoading] = useState(false);
  const [statusFilter, setStatusFilter] = useState();
  const [selectedOrder, setSelectedOrder] = useState(null);
  const [modalVisible, setModalVisible] = useState(false);
  const [files, setFiles] = useState([]);
  const [chatMessages, setChatMessages] = useState([]);
  const [sending, setSending] = useState(false);

  const fetchOrders = useCallback(async () => {
    setLoading(true);
    try {
      const { data } = await axios.get('/api/orders/', { params: { status_filter: statusFilter } });
      setOrders(Array.isArray(data) ? data : []);    } catch {
      message.error('Не удалось загрузить заявки');
    } finally {
      setLoading(false);
    }
  }, [statusFilter]);

  const fetchStats = useCallback(async () => {
    try {
      const { data } = await axios.get('/api/orders/stats');
      setStats(data || { total_orders: 0, new_orders: 0, active_orders: 0 });    } catch {
      setStats({ total_orders: 0, new_orders: 0, active_orders: 0 });
      message.warning('Статистика временно недоступна');
    }
  }, []);

  useEffect(() => {
    fetchOrders();
    fetchStats();
  }, [fetchOrders, fetchStats]);

  const openOrder = async (order) => {
    setSelectedOrder(order);
    setModalVisible(true);
    try {
      const [filesResp, msgResp] = await Promise.all([
        axios.get(`/api/orders/${order.id}/files`),
        axios.get(`/api/orders/${order.id}/messages`),
      ]);
      setFiles(filesResp?.data?.files || []);
      setChatMessages(msgResp?.data?.messages || []);    } catch {
      setFiles([]);
      setChatMessages([]);
      message.warning('Не удалось загрузить файлы или чат по заявке');
    }
  };

  const sendManagerMessage = async (values) => {
    if (!selectedOrder) return;
    const text = (values?.text || '').trim();
    if (!text) return;

    setSending(true);
    try {
      await axios.post(`/api/orders/${selectedOrder.id}/messages`, { text });
      message.success('Сообщение отправлено в Telegram');
      const { data } = await axios.get(`/api/orders/${selectedOrder.id}/messages`);
      setChatMessages(data?.messages || []);    } catch (err) {
      message.error(err?.response?.data?.detail || 'Не удалось отправить сообщение в Telegram');
    } finally {
      setSending(false);
    }
  };

  const updateStatus = async (id, status) => {
    try {
      await axios.put(`/api/orders/${id}`, { status });
      fetchOrders();
    {
      title: 'Клиент',
      render: (_, r) =>
        `${r.full_name || 'Без имени'} (${r.username ? '@' + r.username : 'id:' + r.user_id})`,
    },    { title: 'Тип заявки', dataIndex: 'branch' },
    { title: 'Кратко', dataIndex: 'summary' },
    {
      title: 'Статус',
      render: (_, r) => (
        <Select value={r.status} style={{ width: 150 }} onChange={(v) => updateStatus(r.id, v)}>
          {statusOptions.map((s) => (
            <Option key={s.value} value={s.value}>
              {s.label}
            </Option>
          ))}
        </Select>
      ),    },
    { title: 'Дата', dataIndex: 'created_at', render: (v) => dayjs(v).format('DD.MM.YYYY HH:mm') },
    { title: 'Открыть', render: (_, r) => <Button onClick={() => openOrder(r)}>Карточка</Button> },
  ];

  const parsedPayload = (() => {
    if (!selectedOrder) return {};
    try {
      return JSON.parse(selectedOrder.order_payload || '{}');
    } catch {
      return selectedOrder.order_payload || {};
    }
  })();

  return (
    <div>
      <h1>Заявки Chel3D</h1>

      <Row gutter={12} style={{ marginBottom: 16 }}>
        <Col span={8}>
          <Card>
            <Statistic title='Всего заявок' value={stats.total_orders} prefix={<ShoppingCartOutlined />} />
          </Card>
        </Col>
        <Col span={8}>
          <Card>
            <Statistic title='Новых' value={stats.new_orders} prefix={<Badge dot status='processing' />} />
          </Card>
        </Col>
        <Col span={8}>
          <Card>
            <Statistic title='Активных' value={stats.active_orders} prefix={<SyncOutlined spin />} />
          </Card>
        </Col>      </Row>
      <Space style={{ marginBottom: 12 }}>
        <span>Фильтр:</span>
        <Select allowClear placeholder='Все статусы' style={{ width: 220 }} onChange={setStatusFilter}>
          {statusOptions.map((s) => <Option key={s.value} value={s.value}>{s.label}</Option>)}
        </Select>
      </Space>
      <Table rowKey='id' loading={loading} columns={columns} dataSource={orders} />

      <Space style={{ marginBottom: 12 }}>
        <span>Фильтр:</span>
        <Select allowClear placeholder='Все статусы' style={{ width: 220 }} onChange={setStatusFilter}>
          {statusOptions.map((s) => (
            <Option key={s.value} value={s.value}>
              {s.label}
            </Option>
          ))}
        </Select>

        <Button onClick={() => { fetchOrders(); fetchStats(); }}>Обновить</Button>
      </Space>

      <Table rowKey='id' loading={loading} columns={columns} dataSource={orders} />

      <Modal
        title={`Заявка №${selectedOrder?.id || ''}`}
        open={modalVisible}
        onCancel={() => setModalVisible(false)}
        footer={null}
        width={1000}
      >        {selectedOrder && (
          <Row gutter={16}>
            <Col span={12}>
              <h3>Клиент</h3>
              <p>
                <UserOutlined /> {selectedOrder.full_name || 'Без имени'}
              </p>
              <p>Пользователь: {selectedOrder.username ? `@${selectedOrder.username}` : `id:${selectedOrder.user_id}`}</p>
              <p>Telegram ID: {selectedOrder.user_id}</p>
              <p>Тип: {selectedOrder.branch}</p>
              <Tag color={statusColor[selectedOrder.status]}>
                {statusOptions.find((s) => s.value === selectedOrder.status)?.label || selectedOrder.status}
              </Tag>

              <h3 style={{ marginTop: 16 }}>Параметры</h3>
              <pre style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(parsedPayload, null, 2)}</pre>
            </Col>

            <Col span={12}>
              <h3>Файлы клиента</h3>
              {(files || []).filter((f) => f.file_url).map((f) => (
                <div key={f.id} style={{ marginBottom: 10 }}>
                  <div style={{ marginBottom: 6 }}>{f.original_name || 'Файл'}</div>
                  <Image src={f.file_url} alt={f.original_name} style={{ maxWidth: '100%' }} />
                </div>
              ))}

              <h3 style={{ marginTop: 16 }}>Чат с клиентом</h3>
              <div style={{ maxHeight: 250, overflow: 'auto', border: '1px solid #eee', padding: 8, marginBottom: 8 }}>
                {chatMessages.map((m) => (
                  <p key={m.id}>
                    <b>{m.direction === 'out' ? 'Менеджер' : 'Клиент'}:</b> {m.message_text}
                  </p>
                ))}
              </div>
              <Form onFinish={sendManagerMessage}>
                <Form.Item name='text' rules={[{ required: true, message: 'Введите сообщение' }]}>
                  <Input.TextArea rows={3} placeholder='Введите сообщение клиенту' />
                </Form.Item>
                <Button type='primary' htmlType='submit' loading={sending}>
                  Отправить в Telegram
                </Button>              </Form>
            </Col>
          </Row>
        )}
      </Modal>
    </div>
  );
};

export default Orders;
