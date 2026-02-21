import React, { useEffect, useState } from 'react';
import { Alert, Button, Card, Col, Divider, Form, Input, Row, Switch, Tabs, message } from 'antd';
import { ReloadOutlined, SaveOutlined } from '@ant-design/icons';
import axios from 'axios';

const { TextArea } = Input;

const textFields = {
  general: [
    ['welcome_menu_msg', 'Текст главного меню'],
    ['text_result_prefix', 'Префикс итога заявки'],
    ['text_price_note', 'Строка про стоимость'],
    ['text_submit_ok', 'Сообщение после успешной отправки'],
    ['text_submit_fail', 'Сообщение при ошибке отправки'],
  ],
  menu: [
    ['btn_menu_print', 'Кнопка главного меню: печать'],
    ['btn_menu_scan', 'Кнопка главного меню: сканирование'],
    ['btn_menu_idea', 'Кнопка главного меню: идея'],
    ['btn_menu_about', 'Кнопка главного меню: о нас'],
  ],
  print: [
    ['text_print_tech', 'Описание шага выбора технологии'],
    ['btn_print_fdm', 'Кнопка технологии: FDM'],
    ['btn_print_resin', 'Кнопка технологии: фотополимер'],
    ['btn_print_unknown', 'Кнопка технологии: не знаю'],
    ['text_select_material', 'Описание шага выбора материала'],
    ['btn_mat_petg', 'Кнопка материала: PET-G'],
    ['btn_mat_pla', 'Кнопка материала: PLA'],
    ['btn_mat_petg_carbon', 'Кнопка материала: PET-G Carbon'],
    ['btn_mat_tpu', 'Кнопка материала: TPU'],
    ['btn_mat_nylon', 'Кнопка материала: Нейлон'],
    ['btn_mat_other', 'Кнопка материала: другой'],
    ['btn_resin_standard', 'Кнопка смолы: стандартная'],
    ['btn_resin_abs', 'Кнопка смолы: ABS-Like'],
    ['btn_resin_tpu', 'Кнопка смолы: TPU-Like'],
    ['btn_resin_nylon', 'Кнопка смолы: Нейлон-Like'],
    ['btn_resin_other', 'Кнопка смолы: другая'],
    ['text_describe_material', 'Описание шага «свой материал»'],
    ['text_attach_file', 'Описание шага вложения'],
  ],
  scan: [
    ['text_scan_type', 'Описание шага сканирования'],
    ['btn_scan_human', 'Кнопка скан: человек'],
    ['btn_scan_object', 'Кнопка скан: предмет'],
    ['btn_scan_industrial', 'Кнопка скан: промышленный объект'],
    ['btn_scan_other', 'Кнопка скан: другое'],
  ],
  idea: [
    ['text_idea_type', 'Описание шага идеи'],
    ['btn_idea_photo', 'Кнопка идея: по фото/эскизу'],
    ['btn_idea_award', 'Кнопка идея: сувенир/кубок/медаль'],
    ['btn_idea_master', 'Кнопка идея: мастер-модель'],
    ['btn_idea_sign', 'Кнопка идея: вывески'],
    ['btn_idea_other', 'Кнопка идея: другое'],
    ['text_describe_task', 'Описание шага свободного ввода'],
  ],
  about: [
    ['about_text', 'Описание раздела «О нас»'],
    ['btn_about_equipment', 'Кнопка «Оборудование»'],
    ['btn_about_projects', 'Кнопка «Наши проекты»'],
    ['btn_about_contacts', 'Кнопка «Контакты»'],
    ['btn_about_map', 'Кнопка «На карте»'],
    ['about_equipment_text', 'Текст «Оборудование»'],
    ['about_projects_text', 'Текст «Наши проекты»'],
    ['about_contacts_text', 'Текст «Контакты»'],
    ['about_map_text', 'Текст «На карте»'],
  ],
};

const toggleFields = [
  ['enabled_menu_print', 'Показывать кнопку меню: печать'],
  ['enabled_menu_scan', 'Показывать кнопку меню: сканирование'],
  ['enabled_menu_idea', 'Показывать кнопку меню: идея'],
  ['enabled_menu_about', 'Показывать кнопку меню: о нас'],
  ['enabled_print_fdm', 'Показывать кнопку FDM'],
  ['enabled_print_resin', 'Показывать кнопку фотополимер'],
  ['enabled_print_unknown', 'Показывать кнопку не знаю'],
  ['enabled_scan_human', 'Показывать кнопку скан: человек'],
  ['enabled_scan_object', 'Показывать кнопку скан: предмет'],
  ['enabled_scan_industrial', 'Показывать кнопку скан: промышленный объект'],
  ['enabled_scan_other', 'Показывать кнопку скан: другое'],
  ['enabled_idea_photo', 'Показывать кнопку идея: по фото/эскизу'],
  ['enabled_idea_award', 'Показывать кнопку идея: сувенир/награда'],
  ['enabled_idea_master', 'Показывать кнопку идея: мастер-модель'],
  ['enabled_idea_sign', 'Показывать кнопку идея: вывески'],
  ['enabled_idea_other', 'Показывать кнопку идея: другое'],
  ['enabled_about_equipment', 'Показывать кнопку о нас: оборудование'],
  ['enabled_about_projects', 'Показывать кнопку о нас: проекты'],
  ['enabled_about_contacts', 'Показывать кнопку о нас: контакты'],
  ['enabled_about_map', 'Показывать кнопку о нас: карта'],
];

const photoFields = [
  ['photo_main_menu', 'Фото главного меню'],
  ['photo_print', 'Фото ветки печати'],
  ['photo_scan', 'Фото ветки сканирования'],
  ['photo_idea', 'Фото ветки идеи'],
  ['photo_about', 'Фото раздела о нас'],
  ['photo_about_equipment', 'Фото раздела оборудование'],
  ['photo_about_projects', 'Фото раздела проекты'],
  ['photo_about_contacts', 'Фото раздела контакты'],
  ['photo_about_map', 'Фото раздела карта'],
];

const BotConfig = () => {
  const [loading, setLoading] = useState(false);
  const [textsForm] = Form.useForm();
  const [settingsForm] = Form.useForm();

  const loadConfig = React.useCallback(async () => {
    setLoading(true);
    try {
      const [textsResponse, settingsResponse] = await Promise.all([
        axios.get('/api/bot-config/texts'),
        axios.get('/api/bot-config/settings')
      ]);
      textsForm.setFieldsValue(textsResponse.data || {});
      settingsForm.setFieldsValue(settingsResponse.data || {});
    } catch {
      message.error('Ошибка загрузки настроек');
    } finally {
      setLoading(false);
    }
  }, [settingsForm, textsForm]);

  useEffect(() => {
    loadConfig();
  }, [loadConfig]);

  const saveTexts = async (values) => {
    setLoading(true);
    try {
      await axios.put('/api/bot-config/texts', values);
      message.success('Тексты сохранены');
    } catch {
      message.error('Не удалось сохранить тексты');
    } finally {
      setLoading(false);
    }
  };

  const saveSettings = async (values) => {
    setLoading(true);
    try {
      await axios.put('/api/bot-config/settings', values);
      message.success('Настройки сохранены');
    } catch {
      message.error('Не удалось сохранить настройки');
    } finally {
      setLoading(false);
    }
  };

  const renderTextSection = (title, fields) => (
    <Card title={title} style={{ marginBottom: 12 }}>
      {fields.map(([name, label]) => (
        <Form.Item key={name} label={label} name={name}>
          <TextArea rows={2} />
        </Form.Item>
      ))}
    </Card>
  );

  const tabs = [
    {
      key: 'texts',
      label: '🧩 Конструктор веток и кнопок',
      children: (
        <Form form={textsForm} layout='vertical' onFinish={saveTexts}>
          {renderTextSection('Общие тексты', textFields.general)}
          {renderTextSection('Главное меню', textFields.menu)}
          {renderTextSection('Ветка: Рассчитать печать', textFields.print)}
          {renderTextSection('Ветка: 3D-сканирование', textFields.scan)}
          {renderTextSection('Ветка: Нет модели / Хочу придумать', textFields.idea)}
          {renderTextSection('Ветка: О нас', textFields.about)}
          <Button type='primary' icon={<SaveOutlined />} htmlType='submit' loading={loading}>Сохранить тексты</Button>
        </Form>
      )
    },
    {
      key: 'settings',
      label: '⚙️ Включение кнопок, фото и системные',
      children: (
        <Card title='Системные настройки и фото'>
          <Alert
            type='info'
            showIcon
            style={{ marginBottom: 16 }}
            message='Можно писать любой язык в текстах. Для фото: путь, URL или Telegram file_id.'
          />
          <Form form={settingsForm} layout='vertical' onFinish={saveSettings}>
            <Form.Item label='ID чата/группы для заявок (orders_chat_id)' name='orders_chat_id'>
              <Input />
            </Form.Item>
            <Form.Item label='Юзернейм менеджера (manager_username)' name='manager_username'>
              <Input />
            </Form.Item>
            <Form.Item label='Плейсхолдер по умолчанию (placeholder_photo_path)' name='placeholder_photo_path'>
              <Input />
            </Form.Item>
            <Divider>Фото</Divider>
            {photoFields.map(([name, label]) => (
              <Form.Item key={name} label={label} name={name}><Input /></Form.Item>
            ))}
            <Divider>Включать / выключать кнопки</Divider>
            <Row gutter={16}>
              {toggleFields.map(([name, label]) => (
                <Col span={12} key={name}>
                  <Form.Item label={label} name={name} valuePropName='checked'>
                    <Switch />
                  </Form.Item>
                </Col>
              ))}
            </Row>
            <Button type='primary' icon={<SaveOutlined />} htmlType='submit' loading={loading}>Сохранить настройки</Button>
            <Button style={{ marginLeft: 8 }} icon={<ReloadOutlined />} onClick={loadConfig}>Обновить</Button>
          </Form>
        </Card>
      )
    }
  ];

  return <Tabs items={tabs} />;
};

export default BotConfig;
