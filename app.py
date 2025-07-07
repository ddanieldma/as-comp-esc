import streamlit as st
import pandas as pd
import numpy as np
import subprocess
import sys

# --- Configuração da Página ---
st.set_page_config(
    page_title="Dashboard de Computação Escalável",
    page_icon="⚡",
    layout="wide",
)

# --- Título e Descrição ---
st.title("Análise de Desempenho de Modelos de Paralelismo")
st.markdown("""
Este dashboard controla e exibe os resultados de um experimento que compara três abordagens de processamento paralelo para dados meteorológicos.
**Ambiente de Execução**: Comandos são executados via `nix-shell`.
**Coleta de Métricas**: O tempo de execução é lido de arquivos gerados por cada processo.
""")

# --- Parâmetros do Experimento (Sidebar) ---
st.sidebar.header("Parâmetros do Experimento")
max_parallelism = st.sidebar.number_input(
    "Grau Máximo de Paralelismo",
    min_value=1,
    max_value=16,
    value=4,
    step=1,
    help="Define o número máximo de workers/processos. O experimento rodará de 1 até este valor."
)
num_events = st.sidebar.number_input(
    "Número de Eventos a Gerar",
    min_value=1000,
    value=10000,
    step=1000,
    help="Volume de dados (número de amostras) a serem geradas para o teste."
)


# --- Funções para Executar e Ler Resultados ---

def execute_in_nix_shell(command: str):
    """
    Executa um comando dentro do nix-shell e faz streaming da saída em tempo real
    para o terminal onde o Streamlit está rodando.
    """
    nix_command = f'nix-shell --run "{command}"'
    
    # Usamos Popen para ter controle sobre o processo em execução
    process = subprocess.Popen(
        nix_command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8'
    )

    # Lê a saída (stdout) linha por linha em tempo real
    if process.stdout:
        for line in iter(process.stdout.readline, ''):
            print(line, end='', file=sys.stdout) # Imprime no terminal do Streamlit
            sys.stdout.flush()

    # Espera o processo terminar e captura o stderr
    process.wait()
    stderr_output = ""
    if process.stderr:
        stderr_output = process.stderr.read()

    # Verifica se houve erro na execução
    if process.returncode != 0:
        print(f"ERRO: O comando retornou um código de saída não-zero: {process.returncode}", file=sys.stderr)
        print(f"Stderr:\n{stderr_output}", file=sys.stderr)
        # Levanta uma exceção para que o bloco try/except no dashboard a capture
        raise subprocess.CalledProcessError(
            returncode=process.returncode,
            cmd=nix_command,
            stderr=stderr_output
        )

def run_multiprocessing(workers: int) -> float:
    """Executa a abordagem de multiprocessamento e lê o resultado do CSV."""
    st.write(f"  - Executando Multiprocessamento com {workers} worker(s)...")
    command = f"python mp_pipeline.py --workers {workers}"
    execute_in_nix_shell(command)
    
    try:
        df = pd.read_csv("mp_execution_times.csv")
        # Pega o tempo da última linha que corresponde ao grau de paralelismo testado
        time_value = df[df['degree'] == workers]['execution_time'].iloc[-1]
        return float(time_value)
    except (FileNotFoundError, IndexError, KeyError):
        st.error(f"Não foi possível ler o resultado de 'mp_execution_times.csv' para {workers} worker(s).")
        return np.nan

def run_message_broker(workers: int) -> float:
    """Inicia os containers Docker e lê o resultado do CSV."""
    st.write(f"  - Executando Message Broker com {workers} worker(s)...")
    command_up = f"DEGREE_OF_PARALLELISM={workers} docker-compose up --scale worker={workers} --build --force-recreate"
    command_down = "docker-compose down"
    
    try:
        # O comando docker-compose up pode ser bloqueante.
        # Assumimos que ele termina quando o trabalho acaba.
        execute_in_nix_shell(command_up)
    finally:
        # Garante que os containers serão derrubados mesmo se houver erro
        execute_in_nix_shell(command_down)
        
    try:
        df = pd.read_csv("mbw_execution_times.csv")
        time_value = df[df['degree'] == workers]['execution_time'].iloc[-1]
        return float(time_value)
    except (FileNotFoundError, IndexError, KeyError):
        st.error(f"Não foi possível ler o resultado de 'mbw_execution_times.csv' para {workers} worker(s).")
        return np.nan

def run_spark(workers: int) -> float:
    """Executa a abordagem com Spark e lê o resultado do arquivo de texto."""
    st.write(f"  - Executando Spark com {workers} worker(s)...")
    command = f"spark-submit --master local[{workers}] process_spark.py"
    execute_in_nix_shell(command)
    
    try:
        with open("spark_runtime.txt", "r") as f:
            return float(f.read().strip())
    except FileNotFoundError:
        st.error("Arquivo 'spark_runtime.txt' não encontrado.")
        return np.nan
    except ValueError:
        st.error("Conteúdo de 'spark_runtime.txt' não é um número válido.")
        return np.nan

# --- Lógica Principal do Dashboard ---

if 'results_df' not in st.session_state:
    st.session_state.results_df = pd.DataFrame()

if st.sidebar.button("▶️ Iniciar Experimento", use_container_width=True):
    st.session_state.results_df = pd.DataFrame() # Limpa resultados anteriores

    # Placeholders para a tabela e o gráfico
    results_placeholder = st.empty()
    log_placeholder = st.expander("Logs da Execução", expanded=True)

    with log_placeholder:
        st.info("Iniciando o experimento...")
        try:
            st.write(f"Gerando {num_events:,} eventos para o teste...")
            # Crie o comando para o seu gerador de dados
            # (ajuste o nome do script e o argumento se necessário)
            generate_data_command = f"python generating_data.py --events {num_events}"
            
            execute_in_nix_shell(generate_data_command)
            st.success("Dados gerados com sucesso!")
        except subprocess.CalledProcessError as e:
            st.error("Falha ao gerar os dados. O experimento foi abortado.")
            st.code(f"Erro no script de geração de dados:\n\n{e.stderr}", language="bash")
            st.stop() # Aborta a execução se a geração de dados falhar

    approaches = {
        "Multiprocessamento Local": run_multiprocessing,
        "Message Broker (Docker)": run_message_broker,
        "Apache Spark": run_spark,
    }

    results_data = []

    # Loop principal do experimento
    for degree in range(1, max_parallelism + 1):
        with log_placeholder.container():
            st.write(f"---")
            st.subheader(f"Testando com Grau de Paralelismo = {degree}")

        for name, run_function in approaches.items():
            try:
                with log_placeholder.container():
                    st.write(f"Executando abordagem: **{name}**")
                
                exec_time = run_function(degree)

                results_data.append({
                    "Grau de Paralelismo": degree,
                    "Abordagem": name,
                    "Tempo (s)": exec_time
                })

                # Atualiza o DataFrame e os componentes do dashboard em tempo real
                st.session_state.results_df = pd.DataFrame(results_data)

                with results_placeholder.container():
                    st.write("### Resultados do Experimento")
                    pivot_table = st.session_state.results_df.pivot(
                        index="Grau de Paralelismo",
                        columns="Abordagem",
                        values="Tempo (s)"
                    ).fillna("Pendente")
                    
                    st.write("#### Tabela de Tempos de Execução (em segundos)")
                    st.dataframe(pivot_table, use_container_width=True)

                    # Gráfico de Desempenho
                    if not st.session_state.results_df.empty:
                        # Para o gráfico, é melhor tratar NaNs como 0 ou pular, mas o streamlit lida bem com eles
                        st.write("#### Gráfico: Grau de Paralelismo vs. Tempo")
                        st.line_chart(pivot_table)

            except subprocess.CalledProcessError as e:
                with log_placeholder.container():
                    st.error(f"Erro ao executar {name} com {degree} worker(s):")
                    st.code(f"Comando Falhou: {e.cmd}\n\nStderr:\n{e.stderr}", language="bash")
                results_data.append({"Grau de Paralelismo": degree, "Abordagem": name, "Tempo (s)": np.nan})
            
            except Exception as e:
                with log_placeholder.container():
                    st.error(f"Ocorreu um erro inesperado com {name}: {e}")
                results_data.append({"Grau de Paralelismo": degree, "Abordagem": name, "Tempo (s)": np.nan})
    
    with log_placeholder.container():
        st.success("🎉 Experimento concluído!")

# Exibe os resultados mais recentes se já existirem na sessão
if not st.session_state.results_df.empty:
    with results_placeholder.container():
        st.write("### Resultados do Último Experimento")
        pivot_table = st.session_state.results_df.pivot(
            index="Grau de Paralelismo",
            columns="Abordagem",
            values="Tempo (s)"
        ).fillna("Pendente") # Mostra 'Pendente' se algum valor estiver faltando
        
        st.write("#### Tabela de Tempos de Execução (em segundos)")
        st.dataframe(pivot_table, use_container_width=True)

        st.write("#### Gráfico: Grau de Paralelismo vs. Tempo")
        st.line_chart(pivot_table)