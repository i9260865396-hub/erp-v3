type KpiCardProps = {
  title: string;
  value: string;
  delta?: string;
  hint?: string;
};

export default function KpiCard({ title, value, delta, hint }: KpiCardProps) {
  const isNeg = (delta ?? "").trim().startsWith("-");

  return (
    <div className="relative overflow-hidden rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
      {/* синий акцент как в хедере */}
      <div className="absolute left-0 top-0 h-1 w-full bg-blue-900" />
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-sm font-medium text-slate-700">{title}</div>
          {hint ? <div className="mt-1 text-xs text-slate-500">{hint}</div> : null}
        </div>

        {delta ? (
          <span
            className={[
              "rounded-md px-2 py-1 text-xs font-medium",
              isNeg ? "bg-rose-50 text-rose-700" : "bg-emerald-50 text-emerald-700",
            ].join(" ")}
          >
            {delta}
          </span>
        ) : null}
      </div>

      <div className="mt-3 text-2xl font-semibold tracking-tight">{value}</div>
    </div>
  );
}
