<?php

namespace App;

use Carbon\Carbon;
use Illuminate\Database\Eloquent\Model;

class Transaction extends Model
{
    protected $table = 'transaction';

    protected $primaryKey = 'transaction_id';

    public $timestamps = false;

    protected $casts = [
        'date' => 'datetime',
        'transaction_date' => 'datetime',
    ];

    protected $maps = [
        //'transaction_date' => 'date',
        //'date' => 'transaction_date',
    ];

    public const CURRENCY_SYMBOL = '€';

    public function brand()
    {
        return $this->belongsTo(\App\Brand::class, 'fk_card_brand_id', 'card_brand_id');
    }

    public function pnvtAcqOd()
    {
        return $this->belongsTo(\App\PnvtAcqOd::class, 'pnvt_ACQ_OD_id', 'pnvt_ACQ_OD_id');
    }

    public function payment()
    {
        return $this->belongsTo(\App\Payment::class, 'fk_payment_id', 'payment_id');
    }

    public function scopePayment($query, $paymentId)
    {
        $query->where('fk_payment_id', '=', $paymentId);
    }

    public function scopeCustomer($query, $customer_id)
    {
        $query->where('fk_customer_id', '=', $customer_id);
    }

    public function scopeTerminal($query, $request)
    {
        if ($request->input('terminal', false)) {
            $query->where('TID', '=', $request->terminal);
        }

        return $query;
    }

    public function scopeBetweenDates($query, $request, $defaultStartDate, $defaultEndDate)
    {
        return $query->whereBetween('transaction_date', [Carbon::createFromFormat('d/m/Y', $request->input('startDate') ?: $defaultStartDate)->format('Y-m-d'), Carbon::createFromFormat('d/m/Y', $request->input('endDate') ?: $defaultEndDate)->format('Y-m-d')]);
    }

    public function scopeBetweenDatesInclusive($query, $request, $defaultStartDate, $defaultEndDate)
    {
        $startDate = $request->input('startDate') ? Carbon::createFromFormat('d/m/Y', $request->input('startDate'))->startOfDay()->format('Y-m-d H:i:s') : Carbon::createFromFormat('d/m/Y', $defaultStartDate)->startOfDay()->format('Y-m-d H:i:s');
        $endDate = $request->input('endDate') ? Carbon::createFromFormat('d/m/Y', $request->input('endDate'))->endOfDay()->format('Y-m-d H:i:s') : Carbon::createFromFormat('d/m/Y', $defaultEndDate)->endOfDay()->format('Y-m-d H:i:s');
        $dateColumn = $request->input('dateFilterField') == 'ad' ? 'pnvt_OD_date' : 'transaction_date';

        return $query->whereRaw("$dateColumn BETWEEN ? AND ?", [$startDate, $endDate]);
    }

    public function scopeCardBrand($query, $request)
    {
        if ($request->input('cardBrand', false)) {
            return $query->where('fk_card_brand_id', $request->cardBrand);
        } else {
            return $query;
        }
    }

    public function getMerchantRefeAttribute($value)
    {
        if ($value == 'NULL') {
            $value = str_replace('NULL', '', $value);
        }

        return $value;
    }

    public function getPayedAttribute()
    {
        if ($this->fk_payment_id) {
            return 'paid';
        }

        return '';
    }

    public function getDateAttribute($value)
    {
        return Carbon::parse($value)->format('d/m/Y');
    }

    public function getTransactionDateAttribute($value)
    {
        return Carbon::parse($value)->format('d/m/Y');
    }

    public function getTransactionTimeAttribute($value)
    {
        return Carbon::parse($value)->format('H:i:s');
    }

    public function getTimeAttribute($value)
    {
        return Carbon::parse($value)->format('H:i:s');
    }

    public function getCommissionVatExcludedAttribute()
    {
        return $this->commission_noVAT / 100000;
    }

    public function getCommissionNoVatTotalMonthAttribute($value)
    {
        return number_format($value / 100000, 2, '.', ' ').self::CURRENCY_SYMBOL;
    }

    public function getVatTotalMonthAttribute($value)
    {
        return number_format($value / 100000, 2, '.', ' ').self::CURRENCY_SYMBOL;
    }

    public function getCommissionTotalMonthAttribute($value)
    {
        return number_format($value / 100000, 2, '.', ' ').self::CURRENCY_SYMBOL;
    }

    public function getRawAttribute($value)
    {
        return number_format($value / 100000, 2, '.', ' ').self::CURRENCY_SYMBOL;
    }

    public function getNetAttribute($value)
    {
        return number_format($value / 100000, 2, '.', ' ').self::CURRENCY_SYMBOL;
    }

    public function getNetAmountAttribute($value)
    {
        return self::CURRENCY_SYMBOL.number_format($value / 100000, 2, ',', ' ');
    }

    public function getRawAmountAttribute($value)
    {
        return self::CURRENCY_SYMBOL.number_format($value / 100000, 2, ',', ' ');
    }

    /*
     *  To get vatAmount displayed with currency symbol
     */
    public function getVatDisplayAttribute()
    {
        return self::CURRENCY_SYMBOL.number_format($this->VAT / 100000, 4, ',', ' ');
    }

    /*
     *  To get commissionVatExcluded value displayed with currency symbol
     */
    public function getCommissionVatExcludedDisplayAttribute()
    {
        return self::CURRENCY_SYMBOL.number_format($this->commission_noVAT / 100000, 2, ',', ' ');
    }

    /*
     *  To get commissionVatIncluded value displayed with currency symbol
     */
    public function getCommissionVatIncludedDisplayAttribute()
    {
        return self::CURRENCY_SYMBOL.number_format($this->commission / 100000, 2, ',', ' ');
    }

    public function getCommissionTotalAttribute($value)
    {
        return self::CURRENCY_SYMBOL.number_format($value / 100000, 2, ',', ' ');
    }

    public function getRawTotalAttribute($value)
    {
        return self::CURRENCY_SYMBOL.number_format($value / 100000, 2, ',', ' ');
    }

    public function getNetTotalAttribute($value)
    {
        return self::CURRENCY_SYMBOL.number_format($value / 100000, 2, ',', ' ');
    }

    /**
     * Commission Export
     *
     * @return string
     */
    public function getCommissionVatIncludedExportAttribute()
    {
        return number_format($this->castToFloat($this->commissionVatIncludedDisplay), 6, '.', '');
    }

    public function getNetAmountExportAttribute($value)
    {
        return number_format($this->castToFloat($this->net_amount), 2, '.', '');
    }

    public function getRawAmountExportAttribute($value)
    {
        return number_format($this->castToFloat($this->raw_amount), 6, '.', '');
    }

    public function getVatExportAttribute()
    {
        return number_format($this->castToFloat($this->vatDisplay), 6, '.', '');
    }

    public function getCommissionVatExcludedExportAttribute()
    {
        return number_format($this->castToFloat($this->commissionVatExcludedDisplay), 6, '.', '');

    }

    /**
     * Interchange Amount In Charge Currency
     *
     * @return float
     */
    public function getInterchangeAmountInChargeCurrencyExportAttribute()
    {
        if ($this->Interchange_Amount_in_Charge_Currency) {
            return number_format($this->Interchange_Amount_in_Charge_Currency, 6, '.', ' ');
        }
    }

    public function getInterchangeAmountInChargeCurrencyAttribute($value)
    {
        if ($value) {
            $value = str_replace('-', '', $value);
            if (str_starts_with($value, '.')) {
                $value = '0'.$value;
            }
            $value = floatval($value);
            $value = number_format($value, 6, '.', ' ');
        }

        return $value;
    }

    public function getSchemeFeeAmountInChargeCurrencyAttribute($value)
    {
        if ($value) {
            $value = str_replace('-', '', $value);
            if (str_starts_with($value, '.')) {
                $value = '0'.$value;
            }
            $value = floatval($value);
            $value = number_format($value, 6, '.', ' ');
        }

        return $value;
    }

    public function getInterchangeAmountInChargeCurrencyDisplayAttribute($value)
    {
        if ($this->Interchange_Amount_in_Charge_Currency) {
            return self::CURRENCY_SYMBOL.number_format($this->Interchange_Amount_in_Charge_Currency, 6, ',', ' ');
        }
    }

    public function getSchemeFeeAmountInChargeCurrencyDisplayAttribute($value)
    {
        if ($this->Scheme_Fee_Amount_in_Charge_Currency) {
            return self::CURRENCY_SYMBOL.number_format($this->Scheme_Fee_Amount_in_Charge_Currency, 6, ',', ' ');
        }
    }

    /**
     * Scheme Fee Amount In Charge Currency
     *
     * @return float
     */
    public function getSchemeFeeAmountInChargeCurrencyExportAttribute($value)
    {
        if ($this->Scheme_Fee_Amount_in_Charge_Currency) {
            return number_format($this->Scheme_Fee_Amount_in_Charge_Currency, 6, '.', ' ');
        }
    }

    public function getMerchantServiceChargeCalculationAttribute()
    {
        if ($this->Interchange_Amount_in_Charge_Currency && $this->Scheme_Fee_Amount_in_Charge_Currency) {
            return self::CURRENCY_SYMBOL.number_format($this->commission / 100000 - $this->VAT / 100000 - $this->Interchange_Amount_in_Charge_Currency - $this->Scheme_Fee_Amount_in_Charge_Currency, 6, ',', ' ');
        }

    }

    /**
     * Merchant Service Charge
     *
     * @return float
     */
    public function getMerchantServiceChargeExportAttribute()
    {
        if ($this->Interchange_Amount_in_Charge_Currency && $this->Scheme_Fee_Amount_in_Charge_Currency) {
            return number_format($this->commission / 100000 - $this->VAT / 100000 - $this->Interchange_Amount_in_Charge_Currency - $this->Scheme_Fee_Amount_in_Charge_Currency, 6, '.', ' ');
        }

    }

    private function castToFloat(string $value): float
    {
        return floatval(str_replace(',', '.', str_replace(' ', '', ltrim($value, self::CURRENCY_SYMBOL))));
    }
}
