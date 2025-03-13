WINDOW_WIDTH = 460
WINDOW_HEIGHT = 520
WINDOW_TITLE = "Cursor注册小助手"
BACKUP_DIR = "env_backups"

import os
import sys
import tkinter as tk
from dataclasses import dataclass, field
from pathlib import Path
from tkinter import ttk
from typing import Dict, List, Tuple
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger
from db import NeonDB  # 导入 NeonDB
import glob

console_mode = False

# 配置 loguru 日志
log_path = os.path.join('logs', f'app_{datetime.now().strftime("%Y%m%d")}.log')
os.makedirs('logs', exist_ok=True)
logger.add(log_path, rotation="500 MB", encoding='utf-8')

def sync_accounts_to_db():
    """同步本地账号数据到数据库"""
    try:
        db = NeonDB()
        
        # 同步当前的 .env 文件
        logger.info("正在同步当前账号信息...")
        if os.path.exists('.env'):
            db.import_from_csv('.env')
            logger.info("当前账号信息同步成功")
        
        # 同步备份文件
        backup_dir = 'env_backups'
        if os.path.exists(backup_dir):
            backup_files = glob.glob(os.path.join(backup_dir, 'cursor_account_*.csv'))
            for backup_file in backup_files:
                logger.info(f"正在同步备份文件: {backup_file}")
                db.import_from_csv(backup_file)
            
            if backup_files:
                logger.info(f"成功同步 {len(backup_files)} 个备份文件")
        
        # 获取同步后的账号数量
        accounts = db.get_account_list()
        logger.info(f"数据库中现有账号数量: {len(accounts) if accounts else 0}")
        
        db.close_all()
        return True
    except Exception as e:
        logger.error(f"同步账号数据失败: {str(e)}")
        return False

def test_db_import():
    """测试数据库导入功能"""
    try:
        db = NeonDB()
        
        # 测试导入单个文件
        test_file = 'env_backups/cursor_account_20250313_213022.csv'
        logger.info(f"测试导入文件: {test_file}")
        
        # 读取文件内容
        with open(test_file, 'r', encoding='utf-8') as f:
            content = f.read()
            logger.info(f"文件内容:\n{content}")
        
        # 尝试导入
        if db.import_from_csv(test_file):
            logger.info("文件导入成功")
        else:
            logger.error("文件导入失败")
        
        # 验证导入结果
        accounts = db.get_account_list()
        logger.info(f"数据库中的账号数量: {len(accounts) if accounts else 0}")
        if accounts:
            for account in accounts:
                logger.info(f"账号信息: {account}")
        
        db.close_all()
        return True
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        logger.exception("详细错误信息:")
        return False

# 初始化数据库并同步账号
try:
    db = NeonDB()
    if db.init_database():
        logger.info("数据库初始化成功")
        # 测试数据导入
        if test_db_import():
            logger.info("数据导入测试成功")
        else:
            logger.error("数据导入测试失败")
    else:
        logger.error("数据库初始化失败")
    db.close_all()
except Exception as e:
    logger.error(f"数据库初始化出错: {str(e)}")

from tab import LogWindow, ManageTab, RegisterTab, AboutTab, UI


@dataclass
class WindowConfig:
    width: int = WINDOW_WIDTH
    height: int = WINDOW_HEIGHT
    title: str = WINDOW_TITLE
    backup_dir: str = BACKUP_DIR
    env_vars: List[Tuple[str, str]] = field(default_factory=lambda: [
        ('DOMAIN', '域名'), 
        ('EMAIL', '邮箱'), 
        ('PASSWORD', '密码'),
        ('API_KEY', 'API密钥'),
        ('MOE_MAIL_URL', '邮箱服务地址')
    ])
    buttons: List[Tuple[str, str]] = field(default_factory=lambda: [
        ("生成账号", "generate_account"),
        ("注册账号", "auto_register"),
        ("备份账号", "backup_account")
    ])


class CursorApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.config = WindowConfig()
        self.entries: Dict[str, ttk.Entry] = {}
        self.selected_mode = tk.StringVar(value="admin")

        self.root.title(self.config.title)
        UI.center_window(self.root, self.config.width, self.config.height)
        self.root.resizable(False, False)
        self.root.configure(bg=UI.COLORS['bg'])
        if os.name == 'nt':
            self.root.attributes('-alpha', 0.98)

        UI.setup_styles()
        self.setup_ui()

    def setup_ui(self) -> None:
        main_frame = ttk.Frame(self.root, padding="10", style='TFrame')
        main_frame.pack(fill=tk.BOTH, expand=True)

        content_frame = ttk.Frame(main_frame, style='TFrame')
        content_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False, padx=(0, 0))
        content_frame.configure(width=450)
        content_frame.pack_propagate(False)

        title_label = ttk.Label(
            content_frame,
            text=self.config.title,
            style='Title.TLabel'
        )
        title_label.pack(pady=(0, 6))

        notebook = ttk.Notebook(content_frame)
        notebook.pack(fill=tk.BOTH, expand=True, pady=(0, 2))

        register_tab = RegisterTab(
            notebook,
            env_vars=self.config.env_vars,
            buttons=self.config.buttons,
            entries=self.entries,
            selected_mode=self.selected_mode,
            button_commands={}
        )
        notebook.add(register_tab, text="账号注册")

        manage_tab = ManageTab(notebook)
        notebook.add(manage_tab, text="账号管理")

        about_tab = AboutTab(notebook)
        notebook.add(about_tab, text="关于")

        footer_frame = ttk.Frame(content_frame, style='TFrame')
        footer_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=6)

        footer = ttk.Label(
            footer_frame,
            text="POWERED BY KTO 仅供学习使用",
            style='Footer.TLabel'
        )
        footer.pack(side=tk.LEFT)

        log_button = ttk.Button(
            footer_frame,
            text="日志",
            style='Custom.TButton',
            command=self.toggle_log_window,
            width=10,
            padding=(0, 2, 0, 2)
        )
        log_button.pack(side=tk.RIGHT, padx=(0, 2))

        self.log_window = LogWindow(self.root)

    def toggle_log_window(self):
        if not self.log_window.winfo_viewable():
            self.log_window.show_window()
        else:
            self.log_window.withdraw()


def setup_logging(log_window=None) -> None:
    logger.remove()

    logger.add(
        sink=Path("./cursorRegister_log") / "{time:YYYY-MM-DD}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} |{level:8}| - {message}",
        rotation="50 MB",
        retention="30 days",
        compression="gz",
        enqueue=True,
        backtrace=True,
        diagnose=True,
        level="DEBUG"
    )

    if console_mode:
        logger.add(
            sink=sys.stderr,
            colorize=True,
            enqueue=True,
            backtrace=True,
            diagnose=True,
            level="DEBUG"
        )

    if log_window:
        def gui_sink(message):
            record = message.record
            level = record["level"].name
            text = record["message"]
            log_window.add_log(text, level)

        logger.add(
            sink=gui_sink,
            format="{message}",
            level="DEBUG",
            enqueue=True
        )


def main() -> None:
    try:
        base_path = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(__file__)
        env_path = os.path.join(base_path, '.env')
        if os.path.exists(env_path):
            load_dotenv(dotenv_path=env_path)

        root = tk.Tk()
        app = CursorApp(root)
        setup_logging(app.log_window)
        root.mainloop()
    except Exception as e:
        logger.error(f"程序启动失败: {e}")
        if 'root' in locals():
            UI.show_error(root, "程序启动失败", e)


if __name__ == "__main__":
    main()
