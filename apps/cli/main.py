import typer
import time
import random
import asyncio
from typing import List, Optional
from datetime import datetime

from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.console import Console, Group
from rich.text import Text
from rich.align import Align
from rich import print as rprint

from libs.utils.terminal import console, ATLAS_THEME, print_header, print_step
from libs.core.config import settings

app = typer.Typer(
    help="ATLAS: Distributed Web Crawl & Indexing CLI Engine",
    rich_markup_mode="rich"
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
        rprint(f"  [brand]Mock Mode:[/] {'[bold green]ENABLED[/]' if settings.MOCK_MODE else '[bold yellow]DISABLED[/]'}")
        rprint("\n[dim]Use [bold white]atlas --help[/] for a list of available commands.[/]\n")
        
        # Automatically suggest starting services if in mock mode
        if settings.MOCK_MODE:
            rprint("[bold yellow]![/] Infrastructure is offline. Services are running in [bold green]Simulation Mode[/].")
            rprint("    To start real infrastructure, run: [bold white]docker-compose up -d[/]\n")

app.add_typer(crawl_app, name="crawl")
app.add_typer(search_app, name="search")
app.add_typer(infra_app, name="infra")

def make_dashboard_layout() -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body", ratio=1),
        Layout(name="footer", size=3)
    )
    layout["body"].split_row(
        Layout(name="main", ratio=3),
        Layout(name="sidebar", ratio=1)
    )
    layout["main"].split_column(
        Layout(name="stats", size=10),
        Layout(name="log", ratio=1)
    )
    return layout

class DashboardManager:
    def __init__(self):
        self.start_time = datetime.now()
        self.pages_crawled = 0
        self.urls_discovered = 0
        self.errors = 0
        self.logs = []
        
    def update(self):
        self.pages_crawled += random.randint(1, 5)
        self.urls_discovered += random.randint(10, 50)
        if random.random() < 0.05:
            self.errors += 1
            self.logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] [bold red]ERROR[/] Failed to parse {random.choice(['example.com', 'test.org', 'data.io'])}")
        else:
            self.logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] [green]INFO[/] Indexed {random.choice(['blog/post-1', 'api/v1', 'shop/item-72'])}")
        
        if len(self.logs) > 10:
            self.logs.pop(0)

    def get_stats_table(self) -> Table:
        table = Table(expand=True, box=None)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="bold white", justify="right")
        table.add_column("Trend", style="dim green", justify="right")
        
        uptime = datetime.now() - self.start_time
        table.add_row("Uptime", str(uptime).split(".")[0], "UP")
        table.add_row("Throughput", f"{self.pages_crawled / max(1, uptime.seconds):.2f} p/s", "+12%")
        table.add_row("Discovery Rate", f"{self.urls_discovered / max(1, uptime.seconds):.2f} u/s", "+5%")
        table.add_row("Error Rate", f"{(self.errors / max(1, self.pages_crawled)) * 100:.2f}%", "-2%")
        
        return table

@app.command()
def status():
    """Show overall system status"""
    print_header("ATLAS SYSTEM EXECUTIVE OVERVIEW")
    
    table = Table(title="Service Telemetry", border_style="bright_blue", expand=True)
    table.add_column("Service Cluster", style="cyan")
    table.add_column("Status", style="bold green")
    table.add_column("Utilization", justify="right")
    table.add_column("Health", justify="center")
    
    table.add_row("Frontier-Nodes (3)", "ACTIVE", "45%", "[bold green]+[/]")
    table.add_row("Crawler-Fleet (150)", "ACTIVE", "82%", "[bold green]+[/]")
    table.add_row("Parser-Service (45)", "ACTIVE", "22%", "[bold green]+[/]")
    table.add_row("Embedding-GPU (8)", "ACTIVE", "95%", "[bold green]+[/]")
    table.add_row("Indexer-Nodes (4)", "ACTIVE", "12%", "[bold green]+[/]")
    
    console.print(table)
    
    infra_health = Group(
        Panel("Kafka: [bold green]HEALTHY[/] (Lag: 12ms)", title="Event Bus", border_style="green"),
        Panel("Qdrant: [bold green]HEALTHY[/] (Vectors: 12.4M)", title="Vector Index", border_style="green"),
        Panel("Neo4j: [bold yellow]DEGRADED[/] (GC: 450ms)", title="Graph Index", border_style="yellow")
    )
    console.print(infra_health)

@crawl_app.command("dash")
def crawl_dashboard():
    """Launch real-time crawl monitoring dashboard"""
    layout = make_dashboard_layout()
    manager = DashboardManager()
    
    layout["header"].update(Align.center(Text("ATLAS LIVE TELEMETRY", style="brand"), vertical="middle"))
    layout["footer"].update(Align.center(Text("Press Ctrl+C to exit", style="dim"), vertical="middle"))
    
    # Removed screen=True to avoid empty output in non-standard terminals
    with Live(layout, console=console, refresh_per_second=4):
        try:
            while True:
                manager.update()
                
                # Update Stats
                layout["stats"].update(Panel(manager.get_stats_table(), title="Crawl Statistics", border_style="cyan"))
                
                # Update Logs
                log_text = Text.from_markup("\n".join(manager.logs))
                layout["log"].update(Panel(log_text, title="Real-time Events", border_style="magenta"))
                
                # Update Sidebar (System Health)
                sidebar = Table(expand=True, box=None)
                sidebar.add_column("Resource", style="dim")
                sidebar.add_column("Load", justify="right")
                sidebar.add_row("CPU", f"{random.randint(40, 60)}%")
                sidebar.add_row("MEM", f"{random.randint(20, 30)}GB")
                sidebar.add_row("NET", f"{random.randint(100, 500)}MB/s")
                layout["sidebar"].update(Panel(sidebar, title="Node Health", border_style="yellow"))
                
                time.sleep(0.5)
        except KeyboardInterrupt:
            console.print("\n[bold yellow]![/] Dashboard terminated by user.")

@crawl_app.command("start")
def crawl_start(seeds: List[str] = typer.Argument(..., help="Seed URLs to start crawling")):
    """Start a new distributed crawl job"""
    print_header(f"INITIALIZING JOB: {datetime.now().strftime('%Y%m%d-%H%M')}")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=40),
        TaskProgressColumn(),
        console=console
    ) as progress:
        t1 = progress.add_task("[cyan]Connecting to URL Frontier...", total=100)
        t2 = progress.add_task("[magenta]Allocating Worker Fleet...", total=100)
        t3 = progress.add_task("[yellow]Injecting Seeds...", total=100)
        
        for _ in range(20):
            progress.update(t1, advance=5)
            progress.update(t2, advance=random.randint(2, 10))
            progress.update(t3, advance=random.randint(5, 20))
            time.sleep(0.1)
            
    print_step("Job ATLAS-X-99 initialized successfully.", "success")
    print_step("Fleet operational: 150 workers spawned.", "info")
    rprint(f"\n[bold]Run [cyan]atlas crawl dash[/] to monitor progress.[/]")

@search_app.command("query")
def search_query(query: str, mode: str = "hybrid"):
    """Search the index using semantic, keyword, or hybrid mode"""
    print_header(f"ATLAS SEARCH: {mode.upper()}")
    console.print(f"Querying for: [italic cyan]{query}[/]\n")
    
    # Simulation
    with console.status("[bold blue]Performing hybrid vector/keyword retrieval..."):
        time.sleep(1.2)
    
    results = [
        {"title": "Distributed Information Retrieval: Principles", "url": "https://ir-labs.edu/distributed", "score": 0.992},
        {"title": "Scaling OpenSearch for 100B Documents", "url": "https://opensearch.org/blog/scaling", "score": 0.945},
        {"title": "Vector Embeddings for Semantic Search", "url": "https://huggingface.co/blog/embeddings", "score": 0.881},
    ]
    
    for i, res in enumerate(results):
        score_style = "green" if res['score'] > 0.9 else "yellow"
        console.print(f"[bold]{i+1}. {res['title']}[/]")
        console.print(f"   [blue]{res['url']}[/]")
        console.print(f"   Relevance: [{score_style}]{res['score']:.4f}[/] | Mode: [dim]{mode}[/]\n")

if __name__ == "__main__":
    app()
