# Тестирование Apache Airflow для задач обработки сейсморазведочных данных
## установка зависимостей
```bash
sudo pacman -Syu
sudo pacman -S python python-pip python-virtualenv gcc \
             postgresql-libs openssl libffi \
             sqlite
```

## Создание виртуального окружения
```bash
mkdir -p ~/airflow
cd ~/airflow

python -m venv venv
source venv/bin/activate
pip install --upgrade pip setuptools wheel
```

## Установка Apache Airflow 3

```bash
export AIRFLOW_VERSION=3.1.0
export PYTHON_VERSION=3.13

pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

## Настройка окружения:

В `.zshrc`:
```bash
export AIRFLOW_HOME=~/airflow/airflow_home
```

## Запуск

```bash
airflow standalone
```

Теперь по адресу http://localhost:8080 есть веб интерфейс.

Логин и пароль лежат в `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`


# Настройка пула

Чтобы ограничить число исполнителей:
```
airflow pools set tkm2d_pool 4 "Pool for tkm2d bash tasks"
```
