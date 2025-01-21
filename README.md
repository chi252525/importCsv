
## Python 版本與套件

### Python 需求

Python Version: 3.9.5 以上

## Ubuntu 20 安裝 Python 3.9

參考資料 https://linuxize.com/post/how-to-install-python-3-9-on-ubuntu-20-04/

1. Update the packages list and install the prerequisites:

```bash
sudo apt update
sudo apt install software-properties-common
```
2. Add the deadsnakes PPA to your system’s sources list:

```bash
sudo add-apt-repository ppa:deadsnakes/ppa
```
3. Once the repository is enabled, you can install Python 3.9 by executing:

```bash
sudo apt install python3.9 python3.9-dev python3-pip
```
4. Verify that the installation was successful by typing:

```bash
python3.9 --version
```
---

### Virtualenv 安裝與使用簡介

#### 安裝 virtualenv

```bash
pip3 install virtualenv
```
#### 建立 virtualenv 環境包

```bash
virtualenv venv
```
#### 進入 venv 環境

Linux / MacOS

```bash
source venv/bin/activate
```
Windows

```commandline
./venv/Scripts/activate.bat
```
#### 離開 venv 環境

```bash
deactivate
```
---

### 安裝 Python套件

```bash
pip3 install -r requirements.txt
```
