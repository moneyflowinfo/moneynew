import sys
import pandas as pd
import FinanceDataReader as fdr
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem,
    QVBoxLayout, QWidget, QLabel, QHBoxLayout, QPushButton,
    QHeaderView, QMessageBox, QInputDialog, QScrollArea,
    QComboBox, QLineEdit
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QFont
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pickle
import os

# (file content copied from original - truncated here for brevity)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = FinanceScannerApp()
    win.show()
    sys.exit(app.exec())
