# -*- coding: utf-8 -*-
"""
coletor.py - Coletor automatizado do Radar Fiscal Diario

Coleta noticias recentes de feeds RSS publicos e portais oficiais
relacionados a Reforma Tributaria, agronegocio e fertilizantes - com
cobertura estadual nos 10 estados prioritarios do agronegocio - filtra
por palavras-chave, classifica cada noticia (esfera / uf / setor / impacto)
e mantem um historico rotativo de 7 dias em noticias.json, no formato
consumido pelo index.html.

Instalacao das dependencias:
    pip install requests feedparser beautifulsoup4

Execucao manual:
    python coletor.py

Agendamento:
    - cron (Linux/Mac), rodando todo dia as 7h:
        0 7 * * * /usr/bin/python3 /caminho/completo/coletor.py >> /caminho/completo/coletor.log 2>&1
    - Windows Task Scheduler:
        Acao: python.exe  Argumentos: C:\\caminho\\coletor.py  Iniciar em: C:\\caminho
    - GitHub Actions (.github/workflows/radar.yml):
        on:
          schedule:
            - cron: '0 10 * * *'   # 07:00 horario de Brasilia
        jobs:
          coletar:
            runs-on: ubuntu-latest
            steps:
              - uses: actions/checkout@v4
              - uses: actions/setup-python@v5
                with: {python-version: '3.11'}
              - run: pip install requests feedparser beautifulsoup4
              - run: python coletor.py
              - run: |
                  git config user.name "radar-bot"
                  git config user.email "radar-bot@users.noreply.github.com"
                  git add noticias.json
                  git commit -m "Atualiza noticias.json" || echo "Sem mudancas"
                  git push
"""

import json
import logging
import re
import unicodedata
import urllib.parse
from datetime import datetime, timedelta, timezone, date
from typing import Optional

import feedparser
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("coletor")


def normalizar(texto: str) -> str:
    """Minusculas e sem acento. O codigo-fonte deste arquivo e mantido em
    ASCII puro (sem acentuacao) para evitar problemas de codificacao ao
    salvar/subir o arquivo em editores diferentes; por isso o texto vindo
    das noticias (que tem acento normal) precisa passar por aqui antes de
    ser comparado com as palavras-chave."""
    sem_acento = unicodedata.normalize("NFKD", texto)
    sem_acento = "".join(c for c in sem_acento if not unicodedata.combining(c))
    return sem_acento.lower()


TIMEOUT_SEGUNDOS = 12
JANELA_COLETA_HORAS = 48       # noticias mais antigas que isso nao entram na coleta desta execucao
RETENCAO_DIAS = 7              # historico mantido no noticias.json
LIMITE_ITENS = 35              # itens mais recentes mantidos apos a mesclagem
SAIDA_JSON = "noticias.json"

# ---------------------------------------------------------------------------
# Fontes federais / da Reforma Tributaria
# ---------------------------------------------------------------------------
FONTES_FEDERAIS = [
    {"nome": "Agencia Senado", "url": "https://www12.senado.leg.br/noticias/feed/todasnoticias", "esfera_padrao": "reforma", "uf": None},
    {"nome": "Camara dos Deputados", "url": "https://www.camara.leg.br/noticias/rss", "esfera_padrao": "reforma", "uf": None},
    {"nome": "Agencia Brasil", "url": "https://agenciabrasil.ebc.com.br/feed/", "esfera_padrao": "federal", "uf": None},
    {"nome": "Portal Contabeis", "url": "https://www.contabeis.com.br/rss/noticias/", "esfera_padrao": "federal", "uf": None},
]

# ---------------------------------------------------------------------------
# Mapa de UFs: codigo, nome por extenso, gentilico(s) e orgaos locais.
# Usado tanto para montar a fonte de busca por estado quanto para identificar
# o estado mencionado no titulo/resumo da noticia, mesmo quando a sigla da
# UF nao aparece explicitamente.
# ---------------------------------------------------------------------------
UF_MAPA = {
    "RS": {"nome": "Rio Grande do Sul", "gentilicos": ["gaucho", "gaucha"], "orgaos": ["sefaz-rs", "sefaz rs", "receita estadual do rs"]},
    "SC": {"nome": "Santa Catarina", "gentilicos": ["catarinense"], "orgaos": ["sef/sc", "sef sc", "fazenda catarinense"]},
    "PR": {"nome": "Parana", "gentilicos": ["paranaense"], "orgaos": ["sefa-pr", "sefa pr", "receita estadual do parana"]},
    "SP": {"nome": "Sao Paulo", "gentilicos": ["paulista"], "orgaos": ["sefaz-sp", "sefaz sp", "fazenda paulista"]},
    "MG": {"nome": "Minas Gerais", "gentilicos": ["mineiro", "mineira"], "orgaos": ["sefaz-mg", "sefaz mg", "fazenda mineira"]},
    "GO": {"nome": "Goias", "gentilicos": ["goiano", "goiana"], "orgaos": ["sefaz-go", "sefaz go", "economia-go", "secretaria da economia de goias"]},
    "MS": {"nome": "Mato Grosso do Sul", "gentilicos": ["sul-mato-grossense"], "orgaos": ["sefaz-ms", "sefaz ms"]},
    "MT": {"nome": "Mato Grosso", "gentilicos": ["mato-grossense"], "orgaos": ["sefaz-mt", "sefaz mt"]},
    "MA": {"nome": "Maranhao", "gentilicos": ["maranhense"], "orgaos": ["sefaz-ma", "sefaz ma"]},
    "TO": {"nome": "Tocantins", "gentilicos": ["tocantinense"], "orgaos": ["sefaz-to", "sefaz to"]},
}

# ---------------------------------------------------------------------------
# Fontes estaduais - 10 estados prioritarios do agronegocio.
#
# IMPORTANTE: a maioria das Sefaz estaduais nao publica RSS oficial em
# endereco previsivel (varias nem tem RSS publico), entao em vez de adivinhar
# uma URL de Sefaz que pode nao existir, cada estado usa uma busca do Google
# Noticias (endpoint publico e estavel) filtrada por termos fiscais/agro +
# nome do estado. Isso cobre qualquer veiculo (Sefaz, diario oficial, imprensa
# local) que publique sobre o tema, em vez de depender de uma unica fonte
# oficial que pode estar fora do ar ou nunca ter existido nesse endereco.
# Se a sua Sefaz tiver RSS proprio confirmado, pode trocar a URL por ele.
# ---------------------------------------------------------------------------
def _url_busca_google_news(nome_estado: str) -> str:
    termos = f"(ICMS OR Sefaz OR tributos OR fertilizantes) {nome_estado}"
    termos_codificados = urllib.parse.quote(termos)
    return f"https://news.google.com/rss/search?q={termos_codificados}&hl=pt-BR&gl=BR&ceid=BR:pt-419"


FONTES_ESTADUAIS = [
    {"nome": f"Google Noticias - {dados['nome']}", "url": _url_busca_google_news(dados["nome"]), "esfera_padrao": "estadual", "uf": sigla}
    for sigla, dados in UF_MAPA.items()
]

FONTES = FONTES_FEDERAIS + FONTES_ESTADUAIS

# Palavras/termos de busca. Termos curtos (siglas, sem espaco) sao validados
# com \b...\b para nao colidir como substring de outra palavra; termos com
# espaco (frases) seguem checagem por substring simples, que ja e segura.
PALAVRAS_CHAVE = [
    "fertilizantes", "adubos", "reforma tributaria", "ibs", "cbs",
    "imposto seletivo", "credito presumido", "icms convenio",
    "insumos agricolas", "funrural",
]

GATILHOS_IMPACTO_ALTO = [
    "aliquota", "aumento de imposto", "revogacao", "prazo final",
    "vigencia imediata", "perda de credito", "obrigatoriedade",
]
GATILHOS_IMPACTO_MEDIO = [
    "consulta publica", "consulta", "jurisprudencia", "julgamento",
    "convenio", "prorrogacao",
]
# Tudo que nao bater nos gatilhos acima cai em "baixo" (conceitual / layout de sistema)

PALAVRAS_FERTILIZANTES = ["fertilizante", "adubo", "npk", "ureia", "potassio", "fosfato", "amonia", "enxofre"]
PALAVRAS_MUNICIPAL = ["prefeitura", "municipio", "iss ", "issqn", "nfs-e"]
PALAVRAS_ESTADUAL = ["icms", "sefaz", "convenio", "secretaria da fazenda"]
PALAVRAS_REFORMA = ["reforma tributaria", "imposto seletivo", "comite gestor"]
# ibs/cbs tratados a parte via regex com \b, pois sao siglas curtas


def termo_bate_com_limite_de_palavra(termo: str, texto_lower: str) -> bool:
    """Valida um termo curto (sigla) com limites de palavra, evitando falso
    positivo como substring de outra palavra (ex.: 'GO' dentro de 'GOL')."""
    return re.search(rf"\b{re.escape(termo)}\b", texto_lower, re.IGNORECASE) is not None


def texto_contem_palavra_chave(texto: str) -> bool:
    texto_lower = normalizar(texto)
    for palavra in PALAVRAS_CHAVE:
        if " " in palavra or len(palavra) > 4:
            if palavra in texto_lower:
                return True
        else:
            if termo_bate_com_limite_de_palavra(palavra, texto_lower):
                return True
    return False


def limpar_html(texto: str) -> str:
    """Remove tags HTML de resumos de RSS, retornando texto puro e enxuto."""
    if not texto:
        return ""
    texto_puro = BeautifulSoup(texto, "html.parser").get_text(separator=" ")
    texto_puro = re.sub(r"\s+", " ", texto_puro).strip()
    return texto_puro[:280]


def detectar_uf_por_texto(texto: str) -> Optional[str]:
    """Procura mencao a um dos 10 estados prioritarios no texto: sigla (com
    limite de palavra), nome por extenso, gentilico ou orgao/Sefaz local."""
    texto_lower = normalizar(texto)
    for sigla, dados in UF_MAPA.items():
        if termo_bate_com_limite_de_palavra(sigla, texto_lower):
            return sigla
        if dados["nome"].lower() in texto_lower:
            return sigla
        if any(g in texto_lower for g in dados["gentilicos"]):
            return sigla
        if any(o in texto_lower for o in dados["orgaos"]):
            return sigla
    return None


def classificar_uf(texto: str, uf_da_fonte: Optional[str]) -> Optional[str]:
    """Se a fonte ja e a Sefaz/canal oficial de um estado, usa essa UF
    diretamente; caso contrario, tenta identificar pelo conteudo do texto."""
    if uf_da_fonte:
        return uf_da_fonte
    return detectar_uf_por_texto(texto)


def classificar_esfera(texto: str, esfera_padrao: str, uf: Optional[str]) -> str:
    texto_lower = normalizar(texto)
    if any(p in texto_lower for p in PALAVRAS_REFORMA):
        return "reforma"
    if termo_bate_com_limite_de_palavra("ibs", texto_lower) or termo_bate_com_limite_de_palavra("cbs", texto_lower):
        return "reforma"
    if any(p in texto_lower for p in PALAVRAS_MUNICIPAL):
        return "municipal"
    if uf is not None or any(p in texto_lower for p in PALAVRAS_ESTADUAL):
        return "estadual"
    return esfera_padrao if esfera_padrao in ("federal", "estadual", "municipal", "reforma") else "federal"


def classificar_setor(texto: str) -> str:
    texto_lower = normalizar(texto)
    if any(p in texto_lower for p in PALAVRAS_FERTILIZANTES):
        return "fertilizantes"
    return "agronegocio"


def classificar_impacto(texto: str) -> str:
    texto_lower = normalizar(texto)
    if any(p in texto_lower for p in GATILHOS_IMPACTO_ALTO):
        return "alto"
    if any(p in texto_lower for p in GATILHOS_IMPACTO_MEDIO):
        return "medio"
    return "baixo"


def buscar_feed(url: str) -> Optional[feedparser.FeedParserDict]:
    """Baixa e faz parse de um feed RSS, com tratamento de timeout/erro de rede."""
    try:
        resposta = requests.get(url, timeout=TIMEOUT_SEGUNDOS, headers={"User-Agent": "RadarFiscalBot/1.0"})
        resposta.raise_for_status()
        return feedparser.parse(resposta.content)
    except requests.exceptions.Timeout:
        log.warning("Timeout ao acessar %s", url)
    except requests.exceptions.RequestException as erro:
        log.warning("Falha ao acessar %s: %s", url, erro)
    return None


def data_publicacao(entrada) -> Optional[datetime]:
    """Extrai a data de publicacao de uma entrada de feed, se disponivel."""
    for campo in ("published_parsed", "updated_parsed"):
        valor = getattr(entrada, campo, None)
        if valor:
            return datetime(*valor[:6], tzinfo=timezone.utc)
    return None


def dentro_da_janela_de_coleta(dt_publicacao: Optional[datetime], horas: int = JANELA_COLETA_HORAS) -> bool:
    """Se a data nao estiver disponivel, mantemos a noticia (melhor incluir do que perder)."""
    if dt_publicacao is None:
        return True
    limite = datetime.now(timezone.utc) - timedelta(hours=horas)
    return dt_publicacao >= limite


def coletar() -> list:
    """Percorre todas as fontes (federais + 10 estados) e retorna a lista de
    noticias novas ja filtradas por palavra-chave e classificadas."""
    coletadas = []
    proximo_id = 1

    for fonte in FONTES:
        log.info("Buscando feed: %s", fonte["nome"])
        feed = buscar_feed(fonte["url"])
        if feed is None or not getattr(feed, "entries", None):
            log.warning("Sem entradas para %s - pulando fonte.", fonte["nome"])
            continue

        for entrada in feed.entries:
            titulo = getattr(entrada, "title", "").strip()
            link = getattr(entrada, "link", "").strip()
            resumo_bruto = getattr(entrada, "summary", "") or getattr(entrada, "description", "")
            resumo = limpar_html(resumo_bruto)

            if not titulo or not link:
                continue

            texto_completo = f"{titulo} {resumo}"
            if not texto_contem_palavra_chave(texto_completo):
                continue

            dt_pub = data_publicacao(entrada)
            if not dentro_da_janela_de_coleta(dt_pub):
                continue

            uf = classificar_uf(texto_completo, fonte["uf"])

            item = {
                "id": proximo_id,
                "titulo": titulo,
                "resumo": resumo or "Resumo nao disponivel na fonte original.",
                "setor": classificar_setor(texto_completo),
                "esfera": classificar_esfera(texto_completo, fonte["esfera_padrao"], uf),
                "uf": uf,
                "impacto": classificar_impacto(texto_completo),
                "fonte": fonte["nome"],
                "link": link,
                "data": (dt_pub or datetime.now(timezone.utc)).strftime("%Y-%m-%d"),
            }
            coletadas.append(item)
            proximo_id += 1

    return coletadas


def remover_duplicatas(itens: list) -> list:
    """Remove noticias com o mesmo link, mantendo a primeira ocorrencia da
    lista (o chamador decide a ordem de prioridade antes de passar aqui)."""
    vistos = set()
    unicos = []
    for item in itens:
        if item["link"] in vistos:
            continue
        vistos.add(item["link"])
        unicos.append(item)
    return unicos


def carregar_existentes(caminho: str = SAIDA_JSON) -> list:
    """Carrega o noticias.json ja salvo em execucoes anteriores, se existir."""
    try:
        with open(caminho, "r", encoding="utf-8") as arquivo:
            dados = json.load(arquivo)
            return dados if isinstance(dados, list) else []
    except FileNotFoundError:
        log.info("Nenhum %s preexistente - iniciando historico do zero.", caminho)
        return []
    except json.JSONDecodeError:
        log.warning("%s existente esta corrompido - iniciando historico do zero.", caminho)
        return []


def data_dentro_da_retencao(item: dict, dias: int = RETENCAO_DIAS) -> bool:
    """True se a data do item (YYYY-MM-DD) estiver dentro da janela de retencao."""
    try:
        data_item = datetime.strptime(item["data"], "%Y-%m-%d").date()
    except (KeyError, ValueError):
        return False
    limite = date.today() - timedelta(days=dias)
    return data_item >= limite


def mesclar_com_historico(existentes: list, novas: list, dias: int = RETENCAO_DIAS, limite_itens: int = LIMITE_ITENS) -> list:
    """Mescla noticias ja salvas com as recem-coletadas, deduplica por link
    (priorizando a versao mais nova em caso de conflito), descarta itens fora
    da janela de retencao e mantem apenas os mais recentes."""
    combinadas = remover_duplicatas(novas + existentes)  # novas primeiro: vencem em caso de link repetido
    combinadas = [item for item in combinadas if data_dentro_da_retencao(item, dias)]
    combinadas.sort(key=lambda item: item.get("data", ""), reverse=True)
    combinadas = combinadas[:limite_itens]

    # renumera ids sequencialmente apos a mesclagem
    for novo_id, item in enumerate(combinadas, start=1):
        item["id"] = novo_id

    return combinadas


def salvar_json(itens: list, caminho: str = SAIDA_JSON) -> None:
    with open(caminho, "w", encoding="utf-8") as arquivo:
        json.dump(itens, arquivo, ensure_ascii=False, indent=2)
    log.info("Salvo %s com %d noticias (historico de %d dias).", caminho, len(itens), RETENCAO_DIAS)


def main() -> None:
    log.info("Iniciando coleta do Radar Fiscal Diario (federal + 10 estados prioritarios)...")
    novas = coletar()
    novas = remover_duplicatas(novas)
    log.info("Coleta desta execucao: %d noticias novas relevantes.", len(novas))

    existentes = carregar_existentes()
    combinadas = mesclar_com_historico(existentes, novas)

    if not combinadas:
        log.warning("Nenhuma noticia relevante (nova ou em historico) - noticias.json nao foi sobrescrito.")
        return

    salvar_json(combinadas)
    log.info("Coleta finalizada com sucesso.")


if __name__ == "__main__":
    main()
