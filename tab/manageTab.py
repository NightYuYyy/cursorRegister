TREE_VIEW_HEIGHT = 7
BUTTON_WIDTH = 15
PADDING = {
    'SMALL': 2,
    'MEDIUM': 5,
    'LARGE': 8,
    'XLARGE': 10
}

# 定义列配置
COLUMNS = {
    '域名': {'width': 60},
    '邮箱': {'width': 100},
    '密码': {'width': 80},
    '额度': {'width': 50},
    '剩余天数': {'width': 50}
}

import csv
import glob
import os
import re
import threading
import tkinter as tk
from datetime import datetime, timedelta
from tkinter import ttk, messagebox
from typing import Dict, List, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor
import base64
import json
from pathlib import Path

import requests
from loguru import logger

from utils import CursorManager, error_handler,Utils
from .ui import UI


class ManageTab(ttk.Frame):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, style='TFrame', **kwargs)
        self.observer = None
        self.setup_ui()
        # 初始化时自动刷新列表和使用信息
        self.after(100, self.refresh_list)
        self.after(500, self.auto_update_info)
        # 启动文件监听
        self.start_file_monitoring()

    def setup_ui(self):
        accounts_frame = UI.create_labeled_frame(self, "已保存账号")

        # 创建树形视图
        tree = ttk.Treeview(
            accounts_frame, 
            columns=tuple(COLUMNS.keys()), 
            show='headings', 
            height=TREE_VIEW_HEIGHT
        )

        # 配置每一列
        for col, settings in COLUMNS.items():
            tree.heading(col, text=col)
            tree.column(col, width=settings['width'])

        scrollbar = ttk.Scrollbar(accounts_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)

        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # 绑定点击事件
        tree.bind('<<TreeviewSelect>>', self.on_select)
        tree.bind('<Button-1>', self.on_click)

        # 创建右键菜单
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="复制邮箱", command=lambda: self.copy_to_clipboard('邮箱'))
        self.context_menu.add_command(label="复制密码", command=lambda: self.copy_to_clipboard('密码'))
        tree.bind('<Button-3>', self.show_context_menu)

        # 创建按钮区域
        outer_button_frame = ttk.Frame(self, style='TFrame')
        outer_button_frame.pack(pady=(PADDING['LARGE'], 0), expand=True)

        button_frame = ttk.Frame(outer_button_frame, style='TFrame')
        button_frame.pack(anchor=tk.W)

        # 第一行按钮
        first_row_frame = ttk.Frame(button_frame, style='TFrame')
        first_row_frame.pack(pady=(0, PADDING['MEDIUM']), anchor=tk.W)

        ttk.Button(first_row_frame, text="更新信息", command=self.update_account_info, 
                  style='Custom.TButton', width=BUTTON_WIDTH).pack(side=tk.LEFT, padx=PADDING['MEDIUM'])
        ttk.Button(first_row_frame, text="更换账号", command=self.update_auth, 
                  style='Custom.TButton', width=BUTTON_WIDTH).pack(side=tk.LEFT, padx=PADDING['MEDIUM'])

        # 第二行按钮
        second_row_frame = ttk.Frame(button_frame, style='TFrame')
        second_row_frame.pack(pady=(0, PADDING['XLARGE']), anchor=tk.W)
        
        ttk.Button(second_row_frame, text="重置ID", command=self.reset_machine_id, 
                  style='Custom.TButton', width=BUTTON_WIDTH).pack(side=tk.LEFT, padx=PADDING['MEDIUM'])
        ttk.Button(second_row_frame, text="删除账号", command=self.delete_account, 
                  style='Custom.TButton', width=BUTTON_WIDTH).pack(side=tk.LEFT, padx=PADDING['MEDIUM'])

        self.account_tree = tree
        self.selected_item = None

    def start_file_monitoring(self):
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler
            
            class BackupFolderHandler(FileSystemEventHandler):
                def __init__(self, callback, root):
                    self.callback = callback
                    self.root = root
                    self._last_modified = 0
                
                def on_any_event(self, event):
                    # 防止重复触发
                    current_time = datetime.now().timestamp()
                    if current_time - self._last_modified < 1:  # 1秒内的重复事件将被忽略
                        return
                    self._last_modified = current_time
                    
                    if not event.is_directory and event.src_path.endswith('.csv'):
                        logger.debug(f"检测到文件变化: {event.src_path}, 事件类型: {event.event_type}")
                        # 使用 after 方法确保在主线程中刷新
                        self.root.after(100, self.callback)

            # 创建观察者和事件处理器
            self.observer = Observer()
            event_handler = BackupFolderHandler(self.refresh_list, self)
            
            # 开始监听env_backups文件夹
            backup_path = Path('env_backups')
            if not backup_path.exists():
                backup_path.mkdir(parents=True, exist_ok=True)
            
            self.observer.schedule(event_handler, str(backup_path), recursive=False)
            self.observer.start()
            
            logger.info("已启动文件监听")
        except Exception as e:
            logger.error(f"启动文件监听失败: {str(e)}")

    def __del__(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()
            logger.info("已停止文件监听")

    def on_select(self, event):
        selected_items = self.account_tree.selection()
        if selected_items:
            self.selected_item = selected_items[0]
        else:
            self.selected_item = None

    def on_click(self, event):
        region = self.account_tree.identify("region", event.x, event.y)
        if region == "cell":
            column = self.account_tree.identify_column(event.x)
            column_name = self.account_tree.heading(column)["text"]
            if column_name in ['邮箱', '密码']:
                self.copy_to_clipboard(column_name)

    def show_context_menu(self, event):
        item = self.account_tree.identify_row(event.y)
        if item:
            self.account_tree.selection_set(item)
            self.selected_item = item
            self.context_menu.post(event.x_root, event.y_root)

    def copy_to_clipboard(self, column_name):
        if not self.selected_item:
            return
            
        try:
            value = self.account_tree.item(self.selected_item)['values'][list(COLUMNS.keys()).index(column_name)]
            if value:
                self.clipboard_clear()
                self.clipboard_append(value)
                UI.show_success(self.winfo_toplevel(), f"{column_name}已复制到剪贴板")
                logger.info(f"已复制{column_name}到剪贴板")
        except Exception as e:
            logger.error(f"复制{column_name}失败: {str(e)}")
            UI.show_error(self.winfo_toplevel(), f"复制{column_name}失败", str(e))

    def get_csv_files(self) -> List[str]:
        try:
            return glob.glob('env_backups/cursor_account_*.csv')
        except Exception as e:
            logger.error(f"获取CSV文件列表失败: {str(e)}")
            return []

    def parse_csv_file(self, csv_file: str) -> Dict[str, str]:
        account_data = {
            'DOMAIN': '',
            'EMAIL': '',
            'COOKIES_STR': '',
            'QUOTA': '未知',
            'DAYS': '未知',
            'PASSWORD': '',
            'API_KEY': '',
            'MOE_MAIL_URL': ''
        }
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                next(csv_reader)
                for row in csv_reader:
                    if len(row) >= 2:
                        key, value = row[0], row[1]
                        if key in account_data:
                            account_data[key] = value
        except Exception as e:
            logger.error(f"解析文件 {csv_file} 失败: {str(e)}")
        return account_data

    def update_csv_file(self, csv_file: str, **fields_to_update) -> None:
        if not fields_to_update:
            logger.debug("没有需要更新的字段")
            return

        try:
            rows = []
            with open(csv_file, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                rows = list(csv_reader)

            for field, value in fields_to_update.items():
                field_found = False
                for row in rows:
                    if len(row) >= 2 and row[0] == field:
                        row[1] = str(value)
                        field_found = True
                        break
                if not field_found:
                    rows.append([field, str(value)])

            with open(csv_file, 'w', encoding='utf-8', newline='') as f:
                csv_writer = csv.writer(f)
                csv_writer.writerows(rows)

            logger.debug(f"已更新CSV文件: {csv_file}, 更新字段: {', '.join(fields_to_update.keys())}")
        except Exception as e:
            logger.error(f"更新CSV文件失败: {str(e)}")
            raise

    def refresh_list(self):
        try:
            # 清空现有列表
            for item in self.account_tree.get_children():
                self.account_tree.delete(item)

            # 获取并显示账号列表
            csv_files = self.get_csv_files()
            for csv_file in csv_files:
                account_data = self.parse_csv_file(csv_file)
                self.account_tree.insert('', 'end', iid=csv_file, values=(
                    account_data.get('DOMAIN', ''),
                    account_data.get('EMAIL', ''),
                    account_data.get('PASSWORD', '未知'),
                    account_data.get('QUOTA', '未知'),
                    account_data.get('DAYS', '未知')
                ))

            logger.debug("账号列表已刷新")
        except Exception as e:
            logger.error(f"刷新列表失败: {str(e)}")
            UI.show_error(self.winfo_toplevel(), "刷新列表失败", str(e))

    def get_selected_account(self) -> Tuple[str, Dict[str, str]]:
        if not self.selected_item:
            raise ValueError("请先选择要操作的账号")

        item_values = self.account_tree.item(self.selected_item)['values']
        if not item_values or len(item_values) < 5:
            raise ValueError("所选账号信息不完整")

        csv_file_path = self.selected_item
        account_data = self.parse_csv_file(csv_file_path)

        if not account_data['EMAIL'] or not account_data['PASSWORD'] or not account_data['COOKIES_STR']:
            raise ValueError("账号信息不完整")

        return csv_file_path, account_data

    def handle_account_action(self, action_name: str, action: Callable[[str, Dict[str, str]], None]) -> None:
        try:
            csv_file_path, account_data = self.get_selected_account()
            action(csv_file_path, account_data)
        except Exception as e:
            UI.show_error(self.winfo_toplevel(), f"{action_name}失败", e)
            logger.error(f"{action_name}失败: {str(e)}")

    def get_trial_usage(self, cookie_str: str) -> Tuple[str, str, str, str]:
        if not cookie_str:
            raise ValueError("Cookie信息不能为空")

        try:
            user_id = self.extract_user_id_from_jwt(cookie_str)
            
            if not cookie_str.startswith('WorkosCursorSessionToken='):
                cookie_str = f'WorkosCursorSessionToken={cookie_str}'

            headers = {
                'Cookie': cookie_str,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            }

            timeout = 10
            session = requests.Session()
            session.headers.update(headers)

            def make_request(url: str) -> dict:
                try:
                    response = session.get(url, timeout=timeout)
                    response.raise_for_status()
                    return response.json()
                except requests.RequestException as e:
                    logger.error(f"请求 {url} 失败: {str(e)}")
                    raise ValueError(f"API请求失败: {str(e)}")

            try:
                with ThreadPoolExecutor(max_workers=3) as executor:
                    future_user_info = executor.submit(make_request, "https://www.cursor.com/api/auth/me")
                    future_trial = executor.submit(make_request, "https://www.cursor.com/api/auth/stripe")
                    future_usage = executor.submit(make_request, f"https://www.cursor.com/api/usage?user={user_id}")

                    user_info = future_user_info.result()
                    email = user_info.get('email', '未知')
                    domain = email.split('@')[-1] if '@' in email else '未知'

                    trial_data = future_trial.result()
                    days = str(trial_data.get('daysRemainingOnTrial', '未知'))

                    usage_data = future_usage.result()
                    gpt4_data = usage_data.get('gpt-4', {})
                    used_quota = gpt4_data.get('numRequestsTotal', 0)
                    max_quota = gpt4_data.get('maxRequestUsage', 0)
                    quota = f"{used_quota} / {max_quota}" if max_quota else '未知'

                    return domain, email, quota, days

            except Exception as e:
                logger.error(f"获取账号信息失败: {str(e)}")
                raise ValueError(f"获取账号信息失败: {str(e)}")
            finally:
                session.close()
        except Exception as e:
            logger.error(f"处理 JWT 失败: {str(e)}")
            raise ValueError(f"处理 JWT 失败: {str(e)}")

    def extract_user_id_from_jwt(self, cookies: str) -> str:
        try:
            jwt_token = Utils.extract_token(cookies, "WorkosCursorSessionToken")
            parts = jwt_token.split('.')
            if len(parts) != 3:
                raise ValueError("无效的 JWT 格式")
            
            payload = parts[1]
            payload += '=' * (-len(payload) % 4)
            decoded = base64.b64decode(payload)
            payload_data = json.loads(decoded)
            
            user_id = payload_data.get('sub')
            if not user_id:
                raise ValueError("JWT 中未找到用户 ID")
                
            return user_id
        except Exception as e:
            logger.error(f"从 JWT 提取用户 ID 失败: {str(e)}")
            raise ValueError(f"JWT 解析失败: {str(e)}")

    def update_account_info(self):
        def update_single_account(csv_file_path: str, account_data: Dict[str, str]) -> None:
            cookie_str = account_data.get('COOKIES_STR', '')
            if not cookie_str:
                raise ValueError(f"未找到账号 {account_data['EMAIL']} 的Cookie信息")

            logger.debug(f"开始更新账号信息: {account_data['EMAIL']}")
            logger.debug(f"获取到的cookie字符串长度: {len(cookie_str) if cookie_str else 0}")

            user_id = self.extract_user_id_from_jwt(cookie_str)
            reconstructed_cookie = f"WorkosCursorSessionToken={user_id}%3A%3A{cookie_str.split('%3A%3A')[-1]}" if '%3A%3A' in cookie_str else cookie_str

            domain, email, quota, days = self.get_trial_usage(reconstructed_cookie)
            logger.info(f"成功获取账号信息: 域名={domain}, 邮箱={email}, 额度={quota}, 天数={days}")

            self.account_tree.set(csv_file_path, '域名', domain)
            self.account_tree.set(csv_file_path, '邮箱', email)
            self.account_tree.set(csv_file_path, '密码', account_data.get('PASSWORD', '未知'))
            self.account_tree.set(csv_file_path, '额度', quota)
            self.account_tree.set(csv_file_path, '剩余天数', days)

            try:
                self.update_csv_file(csv_file_path,
                                   DOMAIN=domain,
                                   EMAIL=email,
                                   PASSWORD=account_data.get('PASSWORD', '未知'),
                                   QUOTA=quota,
                                   DAYS=days,
                                   COOKIES_STR=reconstructed_cookie)
            except Exception as e:
                logger.error(f"更新CSV文件失败: {str(e)}")
                raise ValueError(f"更新CSV文件失败: {str(e)}")

            return f"域名: {domain}\n邮箱: {email}\n密码: {account_data.get('PASSWORD', '未知')}\n可用额度: {quota}\n剩余天数: {days}"

        def update_process():
            try:
                self.winfo_toplevel().after(0, lambda: UI.show_loading(
                    self.winfo_toplevel(),
                    "更新账号信息",
                    "正在获取账号信息，请稍候..."
                ))

                success_count = 0
                failed_count = 0
                error_messages = []

                # 确定要更新的账号列表
                if self.selected_item:
                    # 如果有选中项，只更新选中的账号
                    accounts_to_update = [(self.selected_item, self.parse_csv_file(self.selected_item))]
                else:
                    # 如果没有选中项，更新所有账号
                    accounts_to_update = [(file, self.parse_csv_file(file)) for file in self.get_csv_files()]

                # 更新每个账号
                for csv_file, account_data in accounts_to_update:
                    try:
                        result = update_single_account(csv_file, account_data)
                        success_count += 1
                        logger.info(f"成功更新账号: {account_data.get('EMAIL', '')}")
                    except Exception as e:
                        failed_count += 1
                        error_message = f"账号 {account_data.get('EMAIL', '')} 更新失败: {str(e)}"
                        error_messages.append(error_message)
                        logger.error(error_message)

                # 显示更新结果
                self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                
                if success_count > 0 and failed_count == 0:
                    message = f"成功更新 {success_count} 个账号"
                    UI.show_success(self.winfo_toplevel(), message)
                elif success_count > 0 and failed_count > 0:
                    message = f"更新完成\n成功: {success_count} 个\n失败: {failed_count} 个\n\n失败详情:\n" + "\n".join(error_messages)
                    UI.show_warning(self.winfo_toplevel(), "部分更新失败", message)
                else:
                    message = f"所有账号更新失败\n\n失败详情:\n" + "\n".join(error_messages)
                    UI.show_error(self.winfo_toplevel(), "更新失败", message)

            except Exception as e:
                error_message = str(e)
                logger.error(f"更新过程发生错误: {error_message}")
                logger.exception("详细错误信息:")
                self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                self.winfo_toplevel().after(0, lambda: UI.show_error(
                    self.winfo_toplevel(),
                    "更新失败",
                    error_message
                ))

        # 在新线程中执行更新操作
        threading.Thread(target=update_process, daemon=True).start()

    def update_auth(self) -> None:
        def update_account_auth(csv_file_path: str, account_data: Dict[str, str]) -> None:
            cookie_str = account_data.get('COOKIES_STR', '')
            email = account_data.get('EMAIL', '')
            if not cookie_str:
                raise ValueError(f"未找到账号 {email} 的Cookie信息")

            if "WorkosCursorSessionToken=" not in cookie_str:
                cookie_str = f"WorkosCursorSessionToken={cookie_str}"

            def process_auth():
                try:
                    self.winfo_toplevel().after(0, lambda: UI.show_loading(
                        self.winfo_toplevel(),
                        "更换账号",
                        "正在刷新Cookie，请稍候..."
                    ))

                    result = CursorManager().process_cookies(cookie_str, email)
                    
                    self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                    
                    if not result.success:
                        raise ValueError(result.message)

                    UI.show_success(self.winfo_toplevel(), f"账号 {email} 的Cookie已刷新")
                    logger.info(f"已刷新账号 {email} 的Cookie")
                except Exception as e:
                    self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                    self.winfo_toplevel().after(0, lambda: UI.show_error(
                        self.winfo_toplevel(),
                        "更换账号失败",
                        str(e)
                    ))

            threading.Thread(target=process_auth, daemon=True).start()

        self.handle_account_action("刷新Cookie", update_account_auth)

    def delete_account(self):
        def delete_account_file(csv_file_path: str, account_data: Dict[str, str]) -> None:
            confirm_message = (
                f"确定要删除以下账号吗？\n\n"
                f"邮箱：{account_data['EMAIL']}\n"
                f"密码：{account_data['PASSWORD']}\n"
                f"Cookie：{account_data['COOKIES_STR']}"
            )

            if not messagebox.askyesno("确认删除", confirm_message, icon='warning'):
                return

            def process_delete():
                try:
                    self.winfo_toplevel().after(0, lambda: UI.show_loading(
                        self.winfo_toplevel(),
                        "删除账号",
                        "正在删除账号信息，请稍候..."
                    ))

                    os.remove(csv_file_path)
                    self.account_tree.delete(self.selected_item)
                    
                    self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                    logger.info(f"已删除账号: {account_data['EMAIL']}")
                    UI.show_success(self.winfo_toplevel(),
                                    f"已删除账号: {account_data['EMAIL']}")
                except Exception as e:
                    self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                    self.winfo_toplevel().after(0, lambda: UI.show_error(
                        self.winfo_toplevel(),
                        "删除账号失败",
                        str(e)
                    ))

            threading.Thread(target=process_delete, daemon=True).start()

        self.handle_account_action("删除账号", delete_account_file)

    @error_handler
    def reset_machine_id(self) -> None:
        def reset_thread():
            try:
          
                self.after(0, lambda: UI.show_loading(
                    self.winfo_toplevel(),
                    "正在重置机器ID",
                    "正在执行重置操作，请稍候..."
                ))
                
               
                result = CursorManager.reset()
                
               
                self.after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                
                if not result.success:
                    self.after(0, lambda: UI.show_error(
                        self.winfo_toplevel(),
                        "重置机器ID失败",
                        result.message
                    ))
                    return
                    
                self.after(0, lambda: UI.show_success(
                    self.winfo_toplevel(),
                    result.message
                ))
                
            except Exception as e:
               
                self.after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                self.after(0, lambda: UI.show_error(
                    self.winfo_toplevel(),
                    "重置机器ID失败",
                    str(e)
                ))
        
      
        threading.Thread(target=reset_thread, daemon=True).start()

    def auto_update_info(self):
        """静默更新所有账号信息"""
        def update_process():
            try:
                accounts_to_update = [(file, self.parse_csv_file(file)) for file in self.get_csv_files()]
                
                for csv_file, account_data in accounts_to_update:
                    try:
                        cookie_str = account_data.get('COOKIES_STR', '')
                        if not cookie_str:
                            logger.warning(f"账号 {account_data.get('EMAIL', '')} 缺少Cookie信息，跳过更新")
                            continue

                        user_id = self.extract_user_id_from_jwt(cookie_str)
                        reconstructed_cookie = f"WorkosCursorSessionToken={user_id}%3A%3A{cookie_str.split('%3A%3A')[-1]}" if '%3A%3A' in cookie_str else cookie_str

                        domain, email, quota, days = self.get_trial_usage(reconstructed_cookie)
                        
                        self.account_tree.set(csv_file, '域名', domain)
                        self.account_tree.set(csv_file, '邮箱', email)
                        self.account_tree.set(csv_file, '密码', account_data.get('PASSWORD', '未知'))
                        self.account_tree.set(csv_file, '额度', quota)
                        self.account_tree.set(csv_file, '剩余天数', days)

                        self.update_csv_file(csv_file,
                                          DOMAIN=domain,
                                          EMAIL=email,
                                          PASSWORD=account_data.get('PASSWORD', '未知'),
                                          QUOTA=quota,
                                          DAYS=days,
                                          COOKIES_STR=reconstructed_cookie)
                        
                        logger.info(f"自动更新账号成功: {email}")
                    except Exception as e:
                        logger.error(f"自动更新账号 {account_data.get('EMAIL', '')} 失败: {str(e)}")
                        continue

            except Exception as e:
                logger.error(f"自动更新过程发生错误: {str(e)}")
                logger.exception("详细错误信息:")

        # 在新线程中执行更新操作
        threading.Thread(target=update_process, daemon=True).start()
