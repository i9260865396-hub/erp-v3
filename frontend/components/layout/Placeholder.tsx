import Button from "@/components/ui/Button";

export default function Placeholder({ title, subtitle }: { title: string; subtitle: string }) {
  return (
    <div className="space-y-6">
      <div>
        <div className="text-lg font-semibold">{title}</div>
        <div className="mt-1 text-sm text-slate-500">{subtitle}</div>
      </div>

      <div className="rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
        <div className="text-sm font-medium">Раздел в разработке</div>
        <div className="mt-2 text-sm text-slate-500">
          UI уже в твоём стиле (синий header, карточки, таблицы). Дальше подключаем реальные формы и API.
        </div>
        <div className="mt-4 flex flex-wrap gap-2">
          <Button variant="primary">Создать</Button>
          <Button variant="secondary">Импорт</Button>
          <Button variant="secondary">Sync</Button>
          <Button variant="ghost">Настройки</Button>
        </div>
      </div>
    </div>
  );
}
