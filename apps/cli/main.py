import asyncio
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import aiohttp
import typer
from rich import print as rprint
from rich.align import Align
from rich.console import Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from rich.table import Table
from rich.text import Text

from apps.crawler.api import domain_allowed, is_public_hostname_syntax, normalize_domains
from libs.core.config import settings
from libs.schemas.models import CrawlJob, CrawlJobStatus
from libs.utils.terminal import console, print_header, print_step
from services.crawl_jobs.store import CrawlJobStore
from services.url_frontier.manager import URLFrontier

app = typer.Typer(
    help="ATLAS: Distributed Web Crawl & Indexing CLI Engine",
    rich_markup_mode="rich",
)

crawl_app = typer.Typer(help="Crawl management commands")
search_app = typer.Typer(help="Search engine commands")
infra_app = typer.Typer(help="Infrastructure management commands")


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    """
    ATLAS: Distributed Web Intelligence & Search OS
    """
    if ctx.invoked_subcommand is None:
        print_header("ATLAS DISTRIBUTED SEARCH ENGINE")
        rprint("\n[bold cyan]SYSTEM STATUS:[/]")
        rprint(f"  [brand]Version:[/] {settings.VERSION}")
        rprint(
            f"  [brand]Mock Mode:[/] "
            f"{'[bold green]ENABLED[/]' if settings.MOCK_MODE else '[bold yellow]DISABLED[/]'}"
        )
        rprint("\n[dim]Use [bold white]atlas --help[/] for a list of available commands.[/]\n")

        if settings.MOCK_MODE:
            rprint(
                "[bold yellow]![/] Infrastructure is offline. Services are running in "
                "[bold green]Simulation Mode[/]."
            )
            rprint("    To start real infrastructure, run: [bold white]docker-compose up -d[/]\n")


app.add_typer(crawl_app, name="crawl")
app.add_typer(search_app, name="search")
app.add_typer(infra_app, name="infra")


def make_dashboard_layout() -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body", ratio=1),
        Layout(name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(name="main", ratio=3),
        Layout(name="sidebar", ratio=1),
    )
    layout["main"].split_column(
        Layout(name="stats", size=10),
        Layout(name="log", ratio=1),
    )
    return layout


class DashboardManager:
    def __init__(self):
        self.start_time = datetime.now()
        self.pages_crawled = 0
        self.urls_discovered = 0
        self.errors = 0
        self.logs: List[str] = []

    def update_from_frontier(self, stats: Optional[Dict[str, int]] = None):
        if stats and not settings.MOCK_MODE:
            self.pages_crawled = stats.get("completed", 0)
            self.urls_discovered = stats.get("seen", 0)
            self.logs.append(
                f"[{datetime.now().strftime('%H:%M:%S')}] [green]INFO[/] "
                f"Queue={stats.get('queued', 0)} Processing={stats.get('processing', 0)} "
                f"Completed={stats.get('completed', 0)}"
            )
        else:
            self.pages_crawled += random.randint(1, 5)
            self.urls_discovered += random.randint(10, 50)
            if random.random() < 0.05:
                self.errors += 1
                self.logs.append(
                    f"[{datetime.now().strftime('%H:%M:%S')}] [bold red]ERROR[/] "
                    f"Failed to parse {random.choice(['example.com', 'test.org', 'data.io'])}"
                )
            else:
                self.logs.append(
                    f"[{datetime.now().strftime('%H:%M:%S')}] [green]INFO[/] "
                    f"Indexed {random.choice(['blog/post-1', 'api/v1', 'shop/item-72'])}"
                )

        if len(self.logs) > 10:
            self.logs.pop(0)

    def get_stats_table(self) -> Table:
        table = Table(expand=True, box=None)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="bold white", justify="right")
        table.add_column("Trend", style="dim green", justify="right")

        uptime = datetime.now() - self.start_time
        elapsed = max(1, int(uptime.total_seconds()))
        table.add_row("Uptime", str(uptime).split(".")[0], "UP")
        table.add_row("Throughput", f"{self.pages_crawled / elapsed:.2f} p/s", "LIVE")
        table.add_row("Discovery Rate", f"{self.urls_discovered / elapsed:.2f} u/s", "LIVE")
        table.add_row("Error Rate", f"{(self.errors / max(1, self.pages_crawled)) * 100:.2f}%", "OK")

        return table


async def _probe_http(url: str, timeout: float = 2.0) -> bool:
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.get(url) as resp:
                return resp.status < 500
    except Exception:
        return False


async def _gather_live_telemetry() -> Dict[str, Any]:
    frontier_stats: Optional[Dict[str, int]] = None
    active_jobs = 0
    if not settings.MOCK_MODE:
        frontier = URLFrontier()
        store = CrawlJobStore()
        try:
            frontier_stats = await asyncio.wait_for(frontier.stats(), timeout=2.0)
            jobs = await asyncio.wait_for(store.list_jobs(limit=20), timeout=2.0)
            active_jobs = sum(
                1 for j in jobs if j.status in {CrawlJobStatus.QUEUED, CrawlJobStatus.RUNNING}
            )
        except Exception:
            frontier_stats = None
        finally:
            await frontier.close()
            await store.close()

    search_ok, crawl_api_ok = await asyncio.gather(
        _probe_http(f"{settings.SEARCH_API_URL.rstrip('/')}/health"),
        _probe_http(f"{settings.CRAWLER_API_URL.rstrip('/')}/health"),
    )
    return {
        "frontier": frontier_stats,
        "active_jobs": active_jobs,
        "search_api": search_ok,
        "crawler_api": crawl_api_ok,
    }


@app.command()
def status():
    """Show overall system status"""
    print_header("ATLAS SYSTEM EXECUTIVE OVERVIEW")

    telemetry = asyncio.run(_gather_live_telemetry())
    frontier_stats = telemetry["frontier"]

    table = Table(title="Service Telemetry", border_style="bright_blue", expand=True)
    table.add_column("Service Cluster", style="cyan")
    table.add_column("Status", style="bold green")
    table.add_column("Details", justify="right")
    table.add_column("Health", justify="center")

    if frontier_stats is not None:
        table.add_row(
            "URL Frontier (Redis)",
            "ONLINE",
            f"Queued: {frontier_stats['queued']} | Seen: {frontier_stats['seen']}",
            "[bold green]+[/]",
        )
        table.add_row(
            "Crawler Workers",
            "ACTIVE",
            f"In-Flight: {frontier_stats['processing']} | Done: {frontier_stats['completed']}",
            "[bold green]+[/]",
        )
        table.add_row(
            "Crawl Jobs",
            "READY",
            f"Active Jobs: {telemetry['active_jobs']}",
            "[bold green]+[/]",
        )
    else:
        mode_tag = "SIMULATED" if settings.MOCK_MODE else "OFFLINE"
        table.add_row("URL Frontier (Redis)", mode_tag, settings.REDIS_URL, "[bold yellow]~[/]")
        table.add_row("Crawler Workers", mode_tag, "Local / Worker Pool", "[bold yellow]~[/]")

    search_status = "ONLINE" if telemetry["search_api"] else ("SIMULATED" if settings.MOCK_MODE else "OFFLINE")
    crawl_status = "ONLINE" if telemetry["crawler_api"] else ("SIMULATED" if settings.MOCK_MODE else "OFFLINE")
    table.add_row(
        "Search API",
        search_status,
        settings.SEARCH_API_URL,
        "[bold green]+[/]" if telemetry["search_api"] else "[bold yellow]~[/]",
    )
    table.add_row(
        "Crawler API",
        crawl_status,
        settings.CRAWLER_API_URL,
        "[bold green]+[/]" if telemetry["crawler_api"] else "[bold yellow]~[/]",
    )

    console.print(table)

    infra_health = Group(
        Panel(
            f"Bootstrap: [bold white]{settings.KAFKA_SERVERS}[/]",
            title="Event Bus (Redpanda/Kafka)",
            border_style="green",
        ),
        Panel(
            f"OpenSearch: [bold white]{settings.OPENSEARCH_URL}[/] | Qdrant: [bold white]{settings.QDRANT_URL}[/]",
            title="Hybrid Retrieval Stores",
            border_style="green",
        ),
    )
    console.print(infra_health)


@crawl_app.command("dash")
def crawl_dashboard():
    """Launch real-time crawl monitoring dashboard"""
    layout = make_dashboard_layout()
    manager = DashboardManager()

    layout["header"].update(
        Align.center(Text("ATLAS LIVE TELEMETRY", style="brand"), vertical="middle")
    )
    layout["footer"].update(
        Align.center(Text("Press Ctrl+C to exit", style="dim"), vertical="middle")
    )

    with Live(layout, console=console, refresh_per_second=4):
        try:
            while True:
                stats = None
                if not settings.MOCK_MODE:
                    try:
                        telemetry = asyncio.run(_gather_live_telemetry())
                        stats = telemetry["frontier"]
                    except Exception:
                        stats = None
                manager.update_from_frontier(stats)

                layout["stats"].update(
                    Panel(manager.get_stats_table(), title="Crawl Statistics", border_style="cyan")
                )

                log_text = Text.from_markup("\n".join(manager.logs))
                layout["log"].update(
                    Panel(log_text, title="Real-time Events", border_style="magenta")
                )

                sidebar = Table(expand=True, box=None)
                sidebar.add_column("Resource", style="dim")
                sidebar.add_column("Value", justify="right")
                if stats is not None:
                    sidebar.add_row("Queued", str(stats["queued"]))
                    sidebar.add_row("In-Flight", str(stats["processing"]))
                    sidebar.add_row("Completed", str(stats["completed"]))
                else:
                    sidebar.add_row("CPU", f"{random.randint(40, 60)}%")
                    sidebar.add_row("MEM", f"{random.randint(20, 30)}GB")
                    sidebar.add_row("NET", f"{random.randint(100, 500)}MB/s")
                layout["sidebar"].update(
                    Panel(sidebar, title="Node Health", border_style="yellow")
                )

                time.sleep(0.5)
        except KeyboardInterrupt:
            console.print("\n[bold yellow]![/] Dashboard terminated by user.")


async def _submit_crawl_job(
    seeds: List[str],
    max_depth: int,
    max_pages: Optional[int],
    whitelist: List[str],
    blacklist: List[str],
) -> CrawlJob:
    payload = {
        "seeds": seeds,
        "max_depth": max_depth,
        "max_pages": max_pages,
        "domain_whitelist": whitelist,
        "domain_blacklist": blacklist,
    }
    if not settings.MOCK_MODE:
        api_url = f"{settings.CRAWLER_API_URL.rstrip('/')}/crawl"
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5.0)) as session:
                async with session.post(api_url, json=payload) as resp:
                    if resp.status == 201:
                        data = await resp.json()
                        return CrawlJob.model_validate(data)
                    if resp.status == 400:
                        detail = (await resp.json()).get("detail", "Invalid crawl request")
                        raise ValueError(detail)
        except ValueError:
            raise
        except Exception:
            pass

    norm_white = normalize_domains(whitelist)
    norm_black = normalize_domains(blacklist)
    frontier = URLFrontier()
    store = CrawlJobStore()
    try:
        valid_seeds = []
        for raw in seeds:
            normalized = frontier.normalize_url(raw)
            if not normalized:
                continue
            hostname = urlparse(normalized).hostname
            if (
                hostname
                and is_public_hostname_syntax(hostname)
                and domain_allowed(hostname, norm_white, norm_black)
            ):
                valid_seeds.append(normalized)
        valid_seeds = list(dict.fromkeys(valid_seeds))
        if not valid_seeds:
            raise ValueError("No valid public HTTP(S) seeds remain after domain policy validation")

        job = CrawlJob(
            job_id=str(uuid.uuid4()),
            seeds=valid_seeds,
            max_depth=max_depth,
            max_pages=max_pages,
            domain_whitelist=norm_white,
            domain_blacklist=norm_black,
            status=CrawlJobStatus.QUEUED,
            started_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        await store.create(job)
        await frontier.add_urls(valid_seeds, depth=0, job_id=job.job_id)
        return job
    finally:
        await frontier.close()
        await store.close()


@crawl_app.command("start")
def crawl_start(
    seeds: List[str] = typer.Argument(..., help="Seed URLs to start crawling"),
    max_depth: int = typer.Option(3, "--max-depth", "-d", help="Maximum crawl depth"),
    max_pages: Optional[int] = typer.Option(None, "--max-pages", "-p", help="Maximum pages to crawl"),
    whitelist: Optional[List[str]] = typer.Option(None, "--allow-domain", help="Allowed domain(s)"),
    blacklist: Optional[List[str]] = typer.Option(None, "--block-domain", help="Blocked domain(s)"),
):
    """Start a new distributed crawl job"""
    print_header(f"INITIALIZING JOB: {datetime.now().strftime('%Y%m%d-%H%M')}")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=40),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        t1 = progress.add_task("[cyan]Validating & normalizing seeds...", total=100)
        t2 = progress.add_task("[magenta]Persisting job metadata...", total=100)
        t3 = progress.add_task("[yellow]Enqueuing seeds to URL Frontier...", total=100)

        progress.update(t1, advance=100)
        try:
            job = asyncio.run(
                _submit_crawl_job(
                    seeds=seeds,
                    max_depth=max_depth,
                    max_pages=max_pages,
                    whitelist=whitelist or [],
                    blacklist=blacklist or [],
                )
            )
        except Exception as exc:
            print_step(f"Failed to create crawl job: {exc}", "error")
            raise typer.Exit(code=1)

        progress.update(t2, advance=100)
        progress.update(t3, advance=100)

    print_step(f"Job {job.job_id} initialized ({len(job.seeds)} seed(s)).", "success")
    print_step(f"Status: {job.status.value} | Max depth: {job.max_depth}", "info")
    rprint("\n[bold]Run [cyan]atlas crawl dash[/] to monitor progress.[/]")


async def _execute_search(query: str, mode: str, limit: int) -> List[Dict[str, Any]]:
    normalized_mode = mode.lower().strip()
    if normalized_mode not in {"hybrid", "keyword", "semantic"}:
        normalized_mode = "hybrid"

    if not settings.MOCK_MODE:
        url = f"{settings.SEARCH_API_URL.rstrip('/')}/search/{normalized_mode}"
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10.0)) as session:
                async with session.get(url, params={"q": query, "limit": limit}) as resp:
                    if resp.status == 200:
                        payload = await resp.json()
                        return payload.get("results", [])
        except Exception:
            pass

    from apps.search_api.main import search_hybrid, search_keyword, search_semantic

    if normalized_mode == "keyword":
        response = await search_keyword(q=query, limit=limit)
    elif normalized_mode == "semantic":
        response = await search_semantic(q=query, limit=limit)
    else:
        response = await search_hybrid(q=query, limit=limit)

    if response.results:
        return [item.model_dump() for item in response.results]

    if settings.MOCK_MODE:
        return [
            {
                "title": "Distributed Information Retrieval: Principles",
                "url": "https://ir-labs.edu/distributed",
                "score": 0.992,
            },
            {
                "title": "Scaling OpenSearch for 100B Documents",
                "url": "https://opensearch.org/blog/scaling",
                "score": 0.945,
            },
            {
                "title": "Vector Embeddings for Semantic Search",
                "url": "https://huggingface.co/blog/embeddings",
                "score": 0.881,
            },
        ][:limit]
    return []


@search_app.command("query")
def search_query(
    query: str,
    mode: str = typer.Option("hybrid", "--mode", "-m", help="Search mode: hybrid, keyword, or semantic"),
    limit: int = typer.Option(10, "--limit", "-l", help="Maximum results to return"),
):
    """Search the index using semantic, keyword, or hybrid mode"""
    print_header(f"ATLAS SEARCH: {mode.upper()}")
    console.print(f"Querying for: [italic cyan]{query}[/]\n")

    with console.status(f"[bold blue]Performing {mode.lower()} retrieval..."):
        results = asyncio.run(_execute_search(query, mode, limit))

    if not results:
        console.print("[yellow]No matching documents found.[/]")
        return

    for i, res in enumerate(results):
        score = float(res.get("score", 0.0))
        score_style = "green" if score > 0.9 else "yellow"
        console.print(f"[bold]{i + 1}. {res['title']}[/]")
        console.print(f"   [blue]{res['url']}[/]")
        if res.get("snippet"):
            console.print(f"   [dim]{res['snippet'][:160]}[/]")
        console.print(f"   Relevance: [{score_style}]{score:.4f}[/] | Mode: [dim]{mode}[/]\n")


@infra_app.command("config")
def infra_config():
    """Display active infrastructure endpoints and runtime configuration"""
    print_header("ATLAS INFRASTRUCTURE CONFIGURATION")
    table = Table(title="Configured Endpoints", border_style="bright_blue", expand=True)
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="bold white")
    table.add_row("REDIS_URL", settings.REDIS_URL)
    table.add_row("KAFKA_SERVERS", settings.KAFKA_SERVERS)
    table.add_row("OPENSEARCH_URL", settings.OPENSEARCH_URL)
    table.add_row("QDRANT_URL", settings.QDRANT_URL)
    table.add_row("SEARCH_API_URL", settings.SEARCH_API_URL)
    table.add_row("CRAWLER_API_URL", settings.CRAWLER_API_URL)
    table.add_row("EMBEDDING_MODEL", f"{settings.EMBEDDING_MODEL} (dim={settings.EMBEDDING_DIMENSION})")
    table.add_row("MOCK_MODE", str(settings.MOCK_MODE))
    console.print(table)


@infra_app.command("check")
def infra_check():
    """Verify connectivity to ATLAS APIs and backing stores"""
    status()


if __name__ == "__main__":
    app()
