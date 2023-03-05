## Seção 2: Instalação

**Criação da Máquina Virtual:**

Tipo: Linux
Versão:  Ubuntu (64-bit)
Memória base: 14747 MB (min. 4)
Processadores: 5 CPUs (min. 1)
Disco rígido:  novo, a partir de 25 GB, tipo VDI

**Configurações extras:**

Monitor > Memória de  Vídeo:  80 (min. 32) 
Armazenamento > Ascrecentar imagem do ubuntu baixada (ubuntu-22.04.1-desktop-amd64.iso) para o curso em  Controladora: IDE. E remove a vazia.
Geral > Ativados área de transferência compartilhada e arrastar e soltar.
Rede >  Conectado a NAT

Instalação da imagem ubuntu na  VM

Instalação do java: sudo apt install curl mlocate default-jdk -y

Instalação do spark e  ajustes de variaveis em ~/.bashrc

**Inicialização do spark:**
 start-master.sh (em standalone)
/opt/spark/sbin/start-slave.sh  spark://localhost:7077  (worker)


**Acessar spark:**
spark-shell (na linguagem scala)
:quit
pyspark 

Bibliotecas adicionais do Python:
sudo apt install python3-pip

pip install numpy
pip install pandas


**Habilitar SSH  na Máquina Virtual: **
sudo apt update
sudo apt install openssh-server

Configurar as portas - desligar a VM para editar as configurações:
Rede > Avançado > Redirecionamento de portas > Adicionar > 
Nome: SSH
Protocolo: TCP
End. IP Hospedeiro: 127.0.0.1
Porta do Hospedeiro e do Convidado: 22

ip a - valida ip registrado

**Utilização do WinSSH e do WinSCP**


______________________________