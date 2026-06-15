#!/usr/bin/env python3
"""
Kontrakt-/sikkerhetstest for SERVER-SIDE BETALINGSVALIDERING.

Bakgrunn (2026-06-15): betalingsbeløpet kommer rått fra klienten i
/api/checkout/init, /api/vipps/init og /api/checkout/card-* (`amount = int(data.get("amount"))`),
så en manipulert forespørsel kan betale f.eks. 1 kr for en hel ordre.

Denne testen DEFINERER kontrakten for validatoren FØR den implementeres
(TDD). Validatoren skal bruke KUN ordrens egne felter (varer/total/fee/sum/
rabattBelop) — den trenger IKKE replikere pris-byggere/overrides/fiskesuppe,
fordi hver handlekurv-linje allerede bærer ferdig `price`×`qty`. Dermed kan den
ikke gi falske avvisninger av legitime ordrer (de tilfredsstiller reglene per
konstruksjon), samtidig som den fanger det trivielle «betal 1 kr»-angrepet.

Kjør:  python3 tests/test_price_validation.py
(eller med pytest når det er installert)

Implementering (neste steg): legg `validate_order_payment` i en ren modul
`pricing_validation.py` (port av `ref_validate_order_payment` under), og kall den
i de tre betalings-endepunktene. Da bytter testen automatisk fra referanse-
spec til den ekte modulen og må fortsatt være grønn.
"""

import os
import sys

# Gjør det mulig å importere fra repo-roten uansett hvor testen kjøres fra
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

LEGAL_SHIPPING_FEES = {0, 59, 199}


def _num(v, default=0.0):
    try:
        if v is None:
            return float(default)
        return float(v)
    except (TypeError, ValueError):
        return float(default)


def ref_validate_order_payment(order, amount_ore, tol_kr=1):
    """REFERANSE-SPEC. Returnerer (ok: bool, reason: str).

    Validerer at betalingsbeløpet (`amount_ore`, i øre) stemmer med ordren.
    Bruker kun ordrens felter: varer[].price/qty/slug, total, fee, sum, rabattBelop.

    Regler:
      1. Σ(price×qty) for varer (ekskl. slug 'test-produkt') == ordre.total
      2. 0 <= rabattBelop <= total
      3. fee ∈ {0, 59, 199}
      4. implisitt medlemsrabatt (total - rabatt + fee - sum) i [0, total]
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

    # 4) Implisitt medlems-/kode-rabatt (som ikke sendes separat) i fornuftig intervall.
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


# ── Bytt til ekte implementasjon når den finnes ──────────────────────────────
try:
    from pricing_validation import validate_order_payment as _impl  # type: ignore
    VALIDATE = _impl
    USING_IMPL = True
except Exception:
    VALIDATE = ref_validate_order_payment
    USING_IMPL = False


# ── Testdata-hjelpere ────────────────────────────────────────────────────────
def order(varer, total, fee, summ, rabattBelop=0):
    return {"varer": varer, "total": total, "fee": fee, "sum": summ, "rabattBelop": rabattBelop}


def line(price, qty=1, slug="vare", name="Vare"):
    return {"slug": slug, "name": name, "price": price, "qty": qty}


def ore(kr):
    return round(kr * 100)


# ── Testtilfeller ────────────────────────────────────────────────────────────
def test_legit_under_700_fee_199():
    o = order([line(500, 1)], total=500, fee=199, summ=699)   # 500 vare + 199 frakt
    ok, why = VALIDATE(o, ore(699))
    assert ok, why


def test_legit_700_to_1100_fee_59():
    o = order([line(900, 1)], total=900, fee=59, summ=959)
    ok, why = VALIDATE(o, ore(959))
    assert ok, why


def test_legit_over_1100_free_shipping():
    o = order([line(1200, 1)], total=1200, fee=0, summ=1200)
    ok, why = VALIDATE(o, ore(1200))
    assert ok, why


def test_legit_weight_line_price_times_qty():
    # Kveite 800 kr/kg × 0,5 kg = 400 kr på linja
    o = order([line(800, 0.5, slug="kveite", name="Kveite")], total=400, fee=199, summ=599)
    ok, why = VALIDATE(o, ore(599))
    assert ok, why


def test_legit_multi_line_with_discount():
    # 1000 + 500 = 1500 varer, 10% rabatt = 150 → 1350, fraktBase 1350 ≥ 1100 → 0 frakt
    o = order([line(1000), line(500)], total=1500, fee=0, summ=1350, rabattBelop=150)
    ok, why = VALIDATE(o, ore(1350))
    assert ok, why


def test_legit_test_produkt_excluded_from_goods():
    # test-produkt skal IKKE telle med i linjesummen
    o = order([line(500, 1), line(9999, 1, slug="test-produkt", name="Test")],
              total=500, fee=199, summ=699)
    ok, why = VALIDATE(o, ore(699))
    assert ok, why


def test_legit_rounding_tolerance_1kr():
    o = order([line(500, 1)], total=500, fee=199, summ=699)
    ok, why = VALIDATE(o, ore(699) + 80)   # 0,80 kr avrundingssling → innenfor toleranse
    assert ok, why


# ── Angrep / ugyldige ordrer (skal AVVISES) ──────────────────────────────────
def test_fraud_amount_1kr_for_full_order():
    o = order([line(500, 1)], total=500, fee=199, summ=699)
    ok, why = VALIDATE(o, ore(1))           # betaler 1 kr for 699-ordre
    assert not ok, "1 kr-betaling skulle vært avvist"


def test_fraud_amount_zero():
    o = order([line(500, 1)], total=500, fee=199, summ=699)
    ok, _ = VALIDATE(o, 0)
    assert not ok


def test_fraud_lines_do_not_sum_to_total():
    # Klienten har senket linje-prisene men beholdt høy total/sum (inkonsistent)
    o = order([line(10, 1)], total=500, fee=199, summ=699)
    ok, _ = VALIDATE(o, ore(699))
    assert not ok


def test_fraud_amount_off_by_more_than_tolerance():
    o = order([line(500, 1)], total=500, fee=199, summ=699)
    ok, _ = VALIDATE(o, ore(699) - 50_00)   # 50 kr for lite
    assert not ok


def test_invalid_illegal_fee():
    o = order([line(500, 1)], total=500, fee=12345, summ=699)
    ok, _ = VALIDATE(o, ore(699))
    assert not ok


def test_invalid_discount_exceeds_goods():
    o = order([line(500, 1)], total=500, fee=199, summ=199, rabattBelop=900)
    ok, _ = VALIDATE(o, ore(199))
    assert not ok


def test_invalid_sum_inflated_down_by_fake_extra_discount():
    # sum kunstig lav uten reell rabatt → implisitt ekstra-rabatt > total
    o = order([line(500, 1)], total=500, fee=199, summ=50, rabattBelop=0)
    ok, _ = VALIDATE(o, ore(50))
    assert not ok


# ── Dokumentert KJENT begrensning (skal foreløpig PASSERE) ───────────────────
def test_known_gap_consistent_full_tamper_passes():
    """Hvis angriperen senker ALT konsistent (linjer + total + sum), passerer
    konsistens-validatoren — dette er «Attack B» og fanges driftsmessig (admin
    ser mistenkelig lave priser før pakking). Lag-2-vern (sjekk linjepriser mot
    autoritativ produktpris) er en separat, senere oppgave. Denne testen
    dokumenterer at det er en BEVISST gjenstående luke, ikke en regresjon."""
    o = order([line(1, 1)], total=1, fee=199, summ=200)
    ok, _ = VALIDATE(o, ore(200))
    assert ok, "konsistent (men mistenkelig) ordre passerer lag-1 — kjent luke"


# ── Standalone-runner (uten pytest) ──────────────────────────────────────────
def _run():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = failed = 0
    print(f"\nBetalingsvalidering — {len(tests)} tester  "
          f"({'EKTE pricing_validation.py' if USING_IMPL else 'REFERANSE-SPEC (impl mangler ennå)'})\n")
    for t in tests:
        try:
            t()
            passed += 1
            print(f"  ✓ {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  ✗ {t.__name__}  — {e}")
        except Exception as e:
            failed += 1
            print(f"  ✗ {t.__name__}  — UVENTET {type(e).__name__}: {e}")
    print(f"\n  {passed} bestått, {failed} feilet")
    if not USING_IMPL:
        print("\n  ⚠ TODO: implementer `validate_order_payment` i pricing_validation.py")
        print("    (port av ref_validate_order_payment) og kall den i /api/checkout/init,")
        print("    /api/vipps/init og /api/checkout/card-* FØR betaling opprettes.")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run() else 0)
