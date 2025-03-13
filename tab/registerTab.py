DIALOG_WIDTH = 250
DIALOG_HEIGHT = 180
DIALOG_CENTER_WIDTH = 300
DIALOG_CENTER_HEIGHT = 180
BUTTON_WIDTH = 10

import os
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import ttk
from typing import Dict, List, Tuple, Callable

from dotenv import load_dotenv
from loguru import logger

from registerAc import CursorRegistration
from utils import Utils, Result, error_handler, CursorManager
from .ui import UI
from db import NeonDB


class RegisterTab(ttk.Frame):
    def __init__(self, parent, env_vars: List[Tuple[str, str]], buttons: List[Tuple[str, str]],
                 entries: Dict[str, ttk.Entry], selected_mode: tk.StringVar,
                 button_commands: Dict[str, Callable], **kwargs):
        super().__init__(parent, style='TFrame', **kwargs)
        self.env_vars = env_vars
        self.buttons = buttons
        self.entries = entries
        self.selected_mode = selected_mode
        self.selected_mode.set("admin")  # 设置默认为全自动模式
        self.button_commands = button_commands
        self.registrar = None
        self.db = NeonDB()  # 初始化数据库连接
        self.setup_ui()

    def setup_ui(self):
        account_frame = UI.create_labeled_frame(self, "账号信息")
        for row, (var_name, label_text) in enumerate(self.env_vars):
            entry = UI.create_labeled_entry(account_frame, label_text, row)
            if os.getenv(var_name):
                entry.insert(0, os.getenv(var_name))
            self.entries[var_name] = entry

        # 创建隐藏的cookie输入框
        self.entries['cookie'] = ttk.Entry(account_frame)  # 创建但不显示
        if os.getenv('COOKIES_STR'):
            self.entries['cookie'].insert(0, os.getenv('COOKIES_STR'))
        else:
            self.entries['cookie'].insert(0, "WorkosCursorSessionToken")

        button_frame = ttk.Frame(self, style='TFrame')
        button_frame.pack(pady=(8, 0))

        inner_button_frame = ttk.Frame(button_frame, style='TFrame')
        inner_button_frame.pack(expand=True)

        for i, (text, command) in enumerate(self.buttons):
            btn = ttk.Button(
                inner_button_frame,
                text=text,
                command=getattr(self, command),
                style='Custom.TButton',
                width=10
            )
            btn.pack(side=tk.LEFT, padx=10)

    def _save_env_vars(self, updates: Dict[str, str] = None) -> None:
        if not updates:
            updates = {
                var_name: value.strip()
                for var_name, _ in self.env_vars
                if (value := self.entries[var_name].get().strip())
            }

        if updates and not Utils.update_env_vars(updates):
            UI.show_warning(self, "保存环境变量失败")

    @error_handler
    def generate_account(self) -> None:
        def generate_thread():
            try:
                self.winfo_toplevel().after(0, lambda: UI.show_loading(
                    self.winfo_toplevel(),
                    "生成账号",
                    "正在生成账号信息，请稍候..."
                ))

                logger.debug(f"当前环境变量 DOMAIN: {os.getenv('DOMAIN', '未设置')}")
                logger.debug(f"当前环境变量 EMAIL: {os.getenv('EMAIL', '未设置')}")
                logger.debug(f"当前环境变量 PASSWORD: {os.getenv('PASSWORD', '未设置')}")
                
                if domain := self.entries['DOMAIN'].get().strip():
                    if not Utils.update_env_vars({'DOMAIN': domain}):
                        raise RuntimeError("保存域名失败")
                    load_dotenv(override=True)

                if not (result := CursorManager.generate_cursor_account()):
                    raise RuntimeError(result.message)

                email, password = result.data if isinstance(result, Result) else result
                
                self.winfo_toplevel().after(0, lambda: [
                    self.entries[key].delete(0, tk.END) or 
                    self.entries[key].insert(0, value) 
                    for key, value in {'EMAIL': email, 'PASSWORD': password}.items()
                ])

                self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                self.winfo_toplevel().after(0, lambda: UI.show_success(
                    self.winfo_toplevel(),
                    "账号生成成功"
                ))

            except Exception as e:
                self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                self.winfo_toplevel().after(0, lambda: UI.show_error(
                    self.winfo_toplevel(),
                    "生成账号失败",
                    str(e)
                ))

        threading.Thread(target=generate_thread, daemon=True).start()

    @error_handler
    def auto_register(self) -> None:
        self._save_env_vars()
        load_dotenv(override=True)

        def register_thread():
            try:
                self.winfo_toplevel().after(0, lambda: UI.show_loading(
                    self.winfo_toplevel(),
                    "自动注册",
                    "正在执行注册流程，请稍候..."
                ))

                self.registrar = CursorRegistration()
                logger.debug("正在启动注册流程...")

                if token := self.registrar.admin_auto_register():
                    # 更新界面显示
                    self.winfo_toplevel().after(0, lambda: [
                        self.entries['EMAIL'].delete(0, tk.END),
                        self.entries['EMAIL'].insert(0, os.getenv('EMAIL', '未获取到')),
                        self.entries['PASSWORD'].delete(0, tk.END),
                        self.entries['PASSWORD'].insert(0, os.getenv('PASSWORD', '未获取到')),
                        self.entries['cookie'].delete(0, tk.END),
                        self.entries['cookie'].insert(0, f"WorkosCursorSessionToken={token}")
                    ])
                    
                    # 保存到数据库
                    account_data = {
                        'domain': os.getenv('DOMAIN', ''),
                        'email': os.getenv('EMAIL', ''),
                        'password': os.getenv('PASSWORD', ''),
                        'cookies_str': f"WorkosCursorSessionToken={token}",
                        'api_key': os.getenv('API_KEY', ''),
                        'moe_mail_url': os.getenv('MOE_MAIL_URL', '')
                    }
                    
                    if not self.db.add_account(account_data):
                        raise RuntimeError("保存账号到数据库失败")
                    
                    self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                    self.winfo_toplevel().after(0, lambda: UI.show_success(
                        self.winfo_toplevel(),
                        "自动注册成功，账号信息已保存"
                    ))
                    
                    # 触发备份
                    threading.Thread(target=self.backup_account, daemon=True).start()
                else:
                    self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                    self.winfo_toplevel().after(0, lambda: UI.show_warning(
                        self.winfo_toplevel(),
                        "注册流程未完成"
                    ))

            except Exception as e:
                logger.error(f"注册过程发生错误: {str(e)}")
                self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                self.winfo_toplevel().after(0, lambda: UI.show_error(
                    self.winfo_toplevel(),
                    "注册失败",
                    str(e)
                ))
            finally:
                if self.registrar and self.registrar.browser:
                    self.registrar.browser.quit()

        threading.Thread(target=register_thread, daemon=True).start()

    @error_handler
    def backup_account(self) -> None:
        def backup_thread():
            try:
                self.winfo_toplevel().after(0, lambda: UI.show_loading(
                    self.winfo_toplevel(),
                    "备份账号",
                    "正在备份账号信息，请稍候..."
                ))

                # 获取当前账号信息
                email = os.getenv("EMAIL", "")
                if not email:
                    raise ValueError("未找到账号信息，请先注册或更新账号")

                # 从数据库获取账号信息
                account = self.db.get_account_by_email(email)
                if not account:
                    raise ValueError("未找到账号信息")

                # 准备备份数据
                backup_data = {
                    "DOMAIN": account[1],  # domain
                    "EMAIL": account[2],   # email
                    "PASSWORD": account[3], # password
                    "COOKIES_STR": account[4], # cookies_str
                    "API_KEY": account[5], # api_key
                    "MOE_MAIL_URL": account[6], # moe_mail_url
                    "QUOTA": account[7],   # quota
                    "DAYS": account[8]     # days_remaining
                }

                # 创建备份目录
                backup_dir = Path("env_backups")
                backup_dir.mkdir(exist_ok=True)

                # 生成备份文件名
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = backup_dir / f"cursor_account_{timestamp}.csv"

                # 写入备份文件
                with open(backup_path, 'w', encoding='utf-8', newline='') as f:
                    f.write("variable,value\n")
                    for key, value in backup_data.items():
                        if value:
                            f.write(f"{key},{value}\n")

                # 同时保存到数据库备份表
                self.db.backup_account(
                    account_id=account[0],
                    backup_data=backup_data,
                    backup_type="manual",
                    notes=f"手动备份 - {timestamp}"
                )

                self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                logger.info(f"账号信息已备份到: {backup_path}")
                self.winfo_toplevel().after(0, lambda: UI.show_success(
                    self.winfo_toplevel(),
                    f"账号备份成功\n保存位置: {backup_path}"
                ))

            except Exception as e:
                self.winfo_toplevel().after(0, lambda: UI.close_loading(self.winfo_toplevel()))
                logger.error(f"账号备份失败: {str(e)}")
                self.winfo_toplevel().after(0, lambda: UI.show_error(
                    self.winfo_toplevel(),
                    "账号备份失败",
                    str(e)
                ))

        threading.Thread(target=backup_thread, daemon=True).start()

    def __del__(self):
        """清理资源"""
        if hasattr(self, 'db'):
            self.db.close_all()
