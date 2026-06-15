"""Server-side betalingsvalidering — ren modul (ingen Flask-avhengigheter).

Betalingsbeløpet kom tidligere rått fra klienten i /api/checkout/init,
/api/vipps/init og /api/checkout/card-payment-intent, så en manipulert
forespørsel kunne betale f.eks. 1 kr for en hel ordre.

`validate_order_payment` sjekker at betalingsbeløpet stemmer med ordrens egne
felter (varer/total/fee/sum/rabattBelop). Den trenger IKKE replikere
pris-byggere/overrides/fiskesuppe, fordi hver handlekurv-linje allerede bærer
ferdig `price`×`qty`. Derfor gir den ikke falske avvisninger av legitime ordrer,
men fanger «betal-1-kr»-angrepet (mismatch mellom betaling og ordrens sluttsum).

Kontrakten er låst av tests/test_price_validation.py.
"""

LEGAL_SHIPPING_FEES = {0, 59, 199}


def _num(v, default=0.0):
    try:
        if v is None:
            return float(default)
        return float(v)
    except (TypeError, ValueError):
        return float(default)


def validate_order_payment(order, amount_ore, tol_kr=1):
    """Returnerer (ok: bool, reason: str).

    Validerer at `amount_ore` (betaling i øre) stemmer med `order`. Bruker kun
    ordrens felter: varer[].price/qty/slug, total, fee, sum, rabattBelop.

    Regler:
      1. Σ(price×qty) for varer (ekskl. slug 'test-produkt') == ordre.total
      2. 0 <= rabattBelop <= total
      3. fee ∈ {0, 59, 199}
      4. implisitt ekstra-rabatt (total - rabatt + fee - sum) i [0, total]
      5. amount_ore == round(sum * 100)        ← kjerne-sjekken
    Toleranse `tol_kr` (kr) for avrunding.
    """
    varer = order.get("varer") or []

    # 1) Linjesum == oppgitt varebeløp (total)
    goods = 0.0
    for x in varer:
        if (x.get("slug") or "") == "test-produkt":
            continue
        goods += _num(x.get("price")) * _num(x.get("qty"), default=1)
    total = _num(order.get("total"))
    if abs(goods - total) > tol_kr:
        return False, f"linjesum {goods:.2f} != total {total:.2f}"

    # 2) Rabatt innenfor varebeløpet
    rabatt = _num(order.get("rabattBelop"))
    if rabatt < -tol_kr or rabatt > total + tol_kr:
        return False, f"ugyldig rabattBelop {rabatt:.2f} (total {total:.2f})"

    # 3) Frakt er en lovlig verdi
    fee = _num(order.get("fee"))
    if round(fee) not in LEGAL_SHIPPING_FEES:
        return False, f"ulovlig frakt {fee}"

    # 4) Implisitt medlems-/kode-rabatt (sendes ikke separat) i fornuftig intervall.
    #    sum = total - rabattBelop - ekstraRabatt + fee  →  ekstraRabatt = total - rabatt + fee - sum
    summ = _num(order.get("sum"))
    implied_extra = total - rabatt + fee - summ
    if implied_extra < -tol_kr or implied_extra > total + tol_kr:
        return False, f"sum {summ:.2f} inkonsistent (implisitt ekstra-rabatt {implied_extra:.2f})"

    # 5) KJERNE: betalingen må være lik ordrens sluttsum
    expected_ore = round(summ * 100)
    if abs(amount_ore - expected_ore) > tol_kr * 100:
        return False, f"betaling {amount_ore} øre != forventet {expected_ore} øre (sum {summ:.2f})"

    return True, "ok"
