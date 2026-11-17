#!/usr/bin/env bash
# Запуск моста. Первый запуск — интерактивный: MAX попросит SMS-код.
cd "$(dirname "$0")" || exit 1

exec python3 main.py
