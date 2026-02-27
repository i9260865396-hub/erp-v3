import "./globals.css";
import Link from "next/link";

export const metadata = {
  title: "ERP",
  description: "Print ERP",
};

const NavItem = ({ href, label }: { href: string; label: string }) => (
  <Link href={href} className="navItem">
    {label}
  </Link>
);

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="ru">
      <body>
        <div className="appShell">
          <header className="topHeader">
            <div className="brand">
              <div className="brandDot" />
              <div className="brandText">ERP</div>
            </div>

            <div className="topLinks">
              <a className="topLink" href="/materials">Материалы</a>
              <a className="topLink" href="/purchases">Закупки</a>
              <a className="topLink" href="/stock">Склад FIFO</a>
              <a className="topLink" href="/orders">Производство</a>
              <a className="topLink" href="/sales">Продажи/МП</a>
            </div>
          </header>

          <div className="contentGrid">
            <aside className="sideNav">
              <div className="sideTitle">Модули</div>
              <NavItem href="/materials" label="Материалы" />
              <NavItem href="/purchases" label="Закупки" />
              <NavItem href="/stock" label="Склад FIFO" />
              <NavItem href="/orders" label="Производство" />
              <NavItem href="/sales" label="Продажи / МП" />
            </aside>

            <main className="main">{children}</main>
          </div>
        </div>
      </body>
    </html>
  );
}
