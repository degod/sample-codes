<?php

namespace App\Repository\Transaction;

use App\Models\Paynovate\OperationRequest;
use App\Models\Paynovate\Transaction;
use App\Repository\TransactionBlock\TransactionBlockRepositoryInterface;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Auth;
use Illuminate\Support\Facades\Gate;
use Paynovate\DatabaseMigrationsLaravel\ConnectionId;

class TransactionRepository implements TransactionRepositoryInterface
{
    public function __construct(private readonly Transaction $transaction, private readonly OperationRequest $operationRequest, private readonly TransactionBlockRepositoryInterface $transactionBlockRepository) {}

    #[\Override]
    public function all(
        ?int $agent = null,
        ?string $fromDate = null,
        ?string $toDate = null,
        ?int $transactionId = null,
        ?int $tid = null,
        ?string $authCode = null,
        ?int $paymentId = null,
        ?int $customer = null,
        bool $defaultPagination = true,
        string $transactionStatus = 'ALL',
        ?int $amount = null,
        ?string $mid = null,
        int $perPage = 7000,
        int $currentPage = 1,
        ?int $transactionSource = null,
        ?string $amountInCurrency = null,
        ?string $panFirst = null,
        ?string $panLast = null
    ): object {
        if ((is_string($panFirst) || is_string($panLast)) && ($fromDate === null || $toDate === null)) {
            throw new \InvalidArgumentException('$fromDate and $toDate must be set when $panFirst/$panLast is set');
        }

        $data = $this->transaction
            ->when($agent, function ($query, $agent) {
                $query->whereHas('customer', function ($query) use ($agent) {
                    $query->where('fk_portal_user_id', 'like', '%'.$agent.'%');
                });
            })
            ->when($fromDate, function ($query, $fromDate) {
                $query->where('transaction_date', '>=', $fromDate);
            })
            ->when($toDate, function ($query, $toDate) {
                $query->where('transaction_date', '<=', $toDate);
            })
            ->when($transactionId, function ($query, $transactionId) {
                $query->where('transaction_id', '=', $transactionId);
            })
            ->when($tid, function ($query, $tid) {
                $query->where('TID', '=', strval($tid));
            })
            ->when($amountInCurrency, function (Builder $query, string $amountInCurrency) {
                $query->where('amount_in_currency', '=', $amountInCurrency.'000');
            })
            ->when($authCode, function ($query) use ($authCode) {
                $query->where('AUTH_CODE', '=', $authCode);
            })
            ->when($paymentId, function ($query, $paymentId) {
                $query->where('fk_payment_id', '=', $paymentId);
            })
            ->when($transactionSource, function ($query, $transactionSource) {
                $query->where('fk_transaction_source_id', '=', $transactionSource);
            })
            ->when($customer, function ($query, $customer) {
                $query->where('fk_customer_id', '=', $customer);
            })
            ->when(
                $transactionStatus,
                function (Builder $query, string $transactionStatus) use ($transactionId) {
                    if ($transactionStatus === 'ALL') {
                        return;
                    }

                    if ($transactionStatus === 'BLOCKED') {
                        $blockTrans = $this->transactionBlockRepository->getTransactionsArray($transactionId);
                        $query->whereIn('transaction_id', $blockTrans);

                        return;
                    }

                    $operator = $transactionStatus === 'UNPAID' ? '=' : '!=';
                    $query->where('fk_payment_id', $operator, null);
                }
            )
            ->when($amount, function (Builder $query, int $amount) {
                $query->where('raw_amount', '=', $amount);
            })
            ->when($mid, function (Builder $query, string $mid) {
                $query->where('MID', '=', $mid);
            })
            ->when($panFirst || $panLast, function (Builder $query) use ($panFirst, $panLast): void {
                if (is_string($panFirst)) {
                    $query->where('PAN', 'LIKE', "{$panFirst}%");
                }

                if (is_string($panLast)) {
                    $query->where('PAN', 'LIKE', "%{$panLast}%");
                }
            })
            ->orderBy('transaction_id', 'DESC')
            ->when($defaultPagination, fn ($query) => $query->paginate(config('pagination.per_page')), fn ($query) => $query->with('customer', 'payment')->paginate($perPage, ['*'], 'page', $currentPage));

        return $data;
    }

    /**
     * [allBlockTrans description].
     *
     * @param  int|null  $agent  [description]
     * @param  string|null  $myDate  [description]
     * @param  int|null  $transactionId  [description]
     * @param  int|null  $tid  [description]
     * @param  string|null  $authCode  [description]
     * @param  int|null  $paymentId  [description]
     * @param  int|null  $customer  [description]
     * @param  bool|bool  $defaultPagination  [description]
     * @param  int|null  $blockStatus  [description]
     * @param  int|int  $perPage  [description]
     * @param  int|int  $currentPage  [description]
     * @param  int|null  $transactionSource  [description]
     * @return [type] [description]
     */
    #[\Override]
    public function allBlockTrans(
        string $myDate,
        ?int $agent = null,
        ?int $transactionId = null,
        ?int $tid = null,
        ?string $authCode = null,
        ?int $paymentId = null,
        ?int $customer = null,
        bool $defaultPagination = true,
        ?int $blockStatus = null,
        int $perPage = 7000,
        int $currentPage = 1,
        ?int $transactionSource = null
    ): object {
        $blockTrans = [];
        if (! empty($blockStatus)) {
            $blockTrans = $this->transactionBlockRepository->getTransactionsArray($transactionId);
        }

        $data = $this->transaction
            ->join('card_brand', 'card_brand.card_brand_id', '=', 'transaction.fk_card_brand_id')
            ->leftJoin('payment', 'payment.payment_id', '=', 'transaction.fk_payment_id')
            ->leftJoin('payment_file', 'payment_file.payment_file_id', '=', 'payment.fk_payment_file_id')
            ->leftJoin('transaction_block', 'transaction_block.fk_transaction_id', '=', 'transaction.transaction_id')
            ->whereNull('transaction_block.deletion_date')
            ->where('transaction.transaction_date', '=', $myDate)
            ->when($agent, function ($query, $agent) {
                $query->whereHas('transaction.customer', function ($query) use ($agent) {
                    $query->where('transaction.fk_portal_user_id', 'like', '%'.$agent.'%');
                });
            })
            ->when($transactionId, function ($query, $transactionId) {
                $query->where('transaction.transaction_id', '=', $transactionId);
            })
            ->when($tid, function ($query, $tid) {
                $query->where('transaction.TID', '=', strval($tid));
            })
            ->when($authCode, function ($query, $authCode) {
                $query->where('transaction.AUTH_CODE', '=', $authCode);
            })
            ->when($paymentId, function ($query, $paymentId) {
                $query->where('transaction.fk_payment_id', '=', $paymentId);
            })
            ->when($transactionSource, function ($query, $transactionSource) {
                $query->where('transaction.fk_transaction_source_id', '=', $transactionSource);
            })
            ->when($customer, function ($query, $customer) {
                $query->where('transaction.fk_customer_id', '=', $customer);
            })
            ->when($blockStatus, function ($query, $blockStatus) use ($transactionId) {
                $blockTrans = $this->transactionBlockRepository->getTransactionsArray($transactionId);
                if ($blockStatus == 1) {
                    $query->whereIn('transaction.transaction_id', $blockTrans);
                } elseif ($blockStatus == 2) {
                    $query->where('transaction.fk_payment_id', null);
                }
            })
            ->select('transaction.*', 'card_brand.name as card_brand', 'payment.payment_id as payment_id', 'payment_file.payment_file_id as payment_file_id', 'payment_file.name as payment_file_ref', 'transaction_block.transaction_block_id as transaction_block_id')
            ->orderBy('transaction.transaction_id', 'DESC')
            ->when($defaultPagination, fn ($query) => $query->paginate(config('pagination.per_page')), fn ($query) => $query->paginate($perPage, ['*'], 'page', $currentPage));

        return $data;
    }

    /**
     * paymentChecks() returns a joined data from transaction,
     * customer, card_brand, payment and MCC_codes for filtered
     * by a given date and other params.
     */
    #[\Override]
    public function paymentChecks(?string $fromDate = null, ?string $toDate = null, ?string $mcc = null, ?string $paid = null): object
    {
        $query = $this->transaction->query()
            ->select([
                'transaction.transaction_id',
                'transaction.amount_in_currency AS currency_amount',
                'transaction.currency',
                'transaction.raw_amount',
                'card_brand.name AS card_brand',
                'customer.customer_id',
                'customer.legal_name',
                'MCC_codes.MCC',
                'MCC_codes.MCC_description',
                'MCC_codes.warning_amount',
                'MCC_codes.alert_amount',
            ])
            ->leftJoin('customer', 'customer.customer_id', '=', 'transaction.fk_customer_id')
            ->leftJoin('MCC_codes', 'customer.MCC', '=', 'MCC_codes.MCC')
            ->leftJoin('card_brand', 'transaction.fk_card_brand_id', '=', 'card_brand.card_brand_id')
            ->leftJoin('payment', 'payment.payment_id', '=', 'transaction.fk_payment_id')
            ->where('MCC_codes.warning_amount', '>', 0)
            ->where('transaction.raw_amount', '>', \DB::raw('MCC_codes.warning_amount'))
            ->where(function ($query) use ($fromDate, $toDate, $mcc, $paid) {
                if (! empty($fromDate)) {
                    $query->where('transaction.date', '>=', $fromDate);
                }
                if (! empty($toDate)) {
                    $query->where('transaction.date', '<=', $toDate);
                }
                if (! empty($mcc)) {
                    $query->where('customer.MCC', $mcc);
                }
                if (! empty($paid)) {
                    $query->when($paid == 'N', function ($query) {
                        $query->whereNull('payment.payment_id');
                    });
                    $query->when($paid == 'Y', function ($query) {
                        $query->whereNotNull('payment.payment_id');
                    });
                }
            })
            ->orderBy('transaction.raw_amount', 'DESC')
            ->groupBy('transaction.transaction_id')
            ->paginate(config('pagination.per_page'));

        return $query;
    }

    /**
     * A test function to see if it works.
     *
     * @param  string|null  $fromDate  [description]
     * @param  string|null  $toDate  [description]
     * @param  string|null  $mcc  [description]
     * @param  string|null  $paid  [description]
     */
    #[\Override]
    public function getPaymentAlerts(?string $fromDate = null, ?string $toDate = null, ?string $mcc = null, ?string $paid = null): array
    {
        $values = [];
        $begin_date = $fromDate;
        $end_date = $toDate;
        $values['begin_date'] = $fromDate;
        $values['end_date'] = $toDate;
        $values['MCC'] = $mcc;
        $values['paid'] = $paid;

        $sqlQuery = '
            SELECT
                t1.`transaction_id`                                               AS `transaction_id`          ,
                t1.`amount_in_currency`                                           AS `currency_amount`         ,
                t1.`currency`                                                     AS `currency`                ,
                t1.`raw_amount`                                                   AS `raw_amount`              ,
                t5.`name`                                                         AS `card_brand`              ,
                t2.`customer_id`                                                  AS `customer_id`             ,
                t2.`legal_name`                                                   AS `legal_name`              ,
                t4.`MCC`                                                          AS `MCC`                     ,
                t4.`MCC_description`                                              AS `MCC_description`         ,
                t4.`warning_amount`                                               AS `warning_amount`          ,
                t4.`alert_amount`                                                 AS `alert_amount`
            FROM
                (`transaction` AS t1, `customer` AS t2, `MCC_codes` AS t4, `card_brand` AS t5)
                LEFT JOIN `payment` AS t3 ON t3.`payment_id` = t1.`fk_payment_id`
            WHERE
                t1.fk_customer_id = t2.customer_id
            AND
                t1.`fk_card_brand_id` = t5.`card_brand_id`
            AND
                t2.`MCC` = t4.`MCC`
            AND
                t4.`warning_amount` > 0
            AND
                t1.`raw_amount` > t4.`warning_amount`
        ';

        if ($values['MCC']) {
            $sqlQuery .= "
                AND
                    t2.`MCC` = '".$values['MCC']."'
            ";
        }

        if ($values['paid'] == 'N') {
            $sqlQuery .= '
                AND
                    t3.`payment_id` IS NULL
            ';
        } elseif ($values['paid'] == 'Y') {
            $sqlQuery .= '
                AND
                    t3.`payment_id` IS NOT NULL
            ';
        }

        $sqlQuery .= "
            AND
                t1.`date` >= '".$begin_date."'
            AND
                t1.`date` <= '".$end_date."'
            GROUP BY
                t1.`transaction_id`
            ORDER BY
                t1.`raw_amount` DESC
        ";

        $result = \DB::connection(ConnectionId::Paynovate->value)->select($sqlQuery);

        return $result;
    }

    /**
     * Retrieve operation requests based on filters and search criteria.
     *
     * @param  mixed|null  $filter
     * @return \Illuminate\Contracts\Pagination\LengthAwarePaginator
     */
    #[\Override]
    public function getOperationReqeusts(
        ?string $fromDate,
        ?string $toDate,
        ?int $operationRequestId,
        ?int $customerId,
        ?string $customerName,
        ?string $status,
        ?int $paymentId,
        ?string $paymentDate,
        ?string $operationDescription,
        ?string $see_deleted,
        array $select = ['*']
    ): object {
        $operationRequests = $this->operationRequest
            ->when($fromDate, function ($query, $fromDate) {
                $query->where('creation_date', '>=', $fromDate);
            })
            ->when($toDate, function ($query, $toDate) {
                $query->where('creation_date', '<=', $toDate);
            })
            ->when($operationRequestId, function ($query, $operationRequestId) {
                $query->where('operation_request_id', '=', $operationRequestId);
            })
            ->when($customerId, function ($query, $customerId) {
                $query->where('fk_customer_id', '=', $customerId);
            })
            ->when($customerName, function ($query, $customerName) {
                $query->whereHas('customer', function ($query) use ($customerName) {
                    $query->where('legal_name', 'like', '%'.$customerName.'%');
                });
            })
            ->when($status, function ($query) use ($status) {
                $query->where(function ($query) use ($status) {
                    if ($status == 'executed') {
                        $query->whereHas('operation');
                    } elseif ($status == 'cancelled') {
                        $query->whereNotNull('deletion_date');
                    } elseif ($status == 'pending') {
                        $query->whereNull('deletion_date')
                            ->whereDoesntHave('operation');
                    }
                });
            })
            ->when($paymentId, function ($query, $paymentId) {
                $query->whereHas('operation', function ($query) use ($paymentId) {
                    $query->whereHas('operationPayment', function ($query) use ($paymentId) {
                        $query->where('fk_payment_id', '=', $paymentId);
                    });
                });
            })
            ->when($paymentDate, function ($query) use ($paymentDate) {
                $query->whereHas('operation', function ($query) use ($paymentDate) {
                    $query->whereHas('operationPayment', function ($query) use ($paymentDate) {
                        $query->where('creation_date', 'like', '%'.$paymentDate.'%');
                    });

                });
            })
            ->when($operationDescription, function ($query, $operationDescription) {
                $query->where('description', 'like', '%'.$operationDescription.'%');
            })

            ->when(
                (Gate::allows('operation-request.see_deleted') && (isset($see_deleted) && ! empty($see_deleted)))
                || \Auth::user()->is_full_access_user
                || \Auth::user()->super_admin,
                fn ($query) => $query,
                fn ($query) => $query->where('deletion_date', null)
            )
            ->with([
                'operationRequestType',
                'operation',
                'customer',
            ])

            ->select($select)
            ->orderBy('operation_request_id', 'DESC')
            ->paginate(config('pagination.per_page'));

        // Return the paginated operation requests
        return $operationRequests;
    }

    /**
     * Apply filters to the given query.
     *
     * @param  \Illuminate\Database\Eloquent\Builder  $query
     * @param  array  $filterBy
     * @param  string  $prefix
     */
    private function applyFilters($query, mixed $filter, $filterBy, $prefix = '')
    {
        // Iterate through each filter condition and apply it to the query
        foreach ($filterBy as $item) {
            $column = $item[0];
            $operator = $item[1] ?? '='; // Default to '=' if operator is not specified
            if ($operator === 'like') {
                $query->orWhere($prefix.$column, 'like', '%'.$filter.'%');
            } else {
                $query->orWhere($prefix.$column, $operator, $filter);
            }
        }
    }

    #[\Override]
    public function softDeleteOpReq(string $id)
    {
        $operationRequest = $this->operationRequest->where('operation_request_id', $id)->first();
        $operationRequest->deletion_date = Carbon::now();
        $operationRequest->last_change = Carbon::now();
        $operationRequest->save();

        // Return the updated OperationRequest instance
        return $operationRequest;
    }

    public function softRestoreOpReq(string $id)
    {
        $operationRequest = $this->operationRequest->where('operation_request_id', $id)->first();
        $operationRequest->deletion_date = null;
        $operationRequest->last_change = Carbon::now();
        $operationRequest->save();

        // Return the updated OperationRequest instance
        return $operationRequest;
    }

    /**
     * Store a new OperationRequest with the provided data.
     *
     * @param  array  $request  The data to create the new OperationRequest.
     * @return OperationRequest The newly created OperationRequest instance.
     */
    #[\Override]
    public function store(array $request)
    {
        // Get the last primary key value
        $lastPrimaryKey = OperationRequest::max('operation_request_id') ?? 0;

        // Calculate the new primary key value by adding 10 to the last primary key
        $newPrimaryKey = $request['operation_request_id'] ?? ($lastPrimaryKey + 10);

        $amount = ! empty($request['amount']) ? ($request['amount'] * 100000) : 0;
        // Create a new OperationRequest instance with the provided data
        $operationRequest = OperationRequest::create([
            'operation_request_id' => $newPrimaryKey,
            'is_case_refund' => ! empty($request['is_case_refund']) && $request['is_case_refund'] == 'on' ? 1 : 0,
            'fk_operation_request_type_id' => $request['operation_request_type_id'] ?? null,
            'fk_operation_request_customer_case_id' => null,
            'fk_customer_id' => $request['customer-id'] ?? null,
            'created_by' => Auth::user()->id,
            'fk_user_id' => 0,
            'amount' => isset($request['operation_type']) ? ($request['operation_type'] == 1 ? abs($amount) : -abs($amount)) : $amount,
            'description' => $request['operation_desc'] ?? null,
            'creation_date' => Carbon::now(),
            'date' => Carbon::now(),
        ]);

        // Return the newly created OperationRequest instance
        return $operationRequest;
    }

    /**
     * Update an existing OperationRequest with the provided data.
     *
     * @param  array  $request  The data to update the OperationRequest.
     * @param  OperationRequest  $operationRequest  The OperationRequest instance to be updated.
     * @return OperationRequest The updated OperationRequest instance.
     */
    public function update(array $request, OperationRequest $operationRequest)
    {
        $operationRequest->is_case_refund = ! empty($request['is_case_refund']) && $request['is_case_refund'] == 'on' ? 1 : 0;
        // Set the operation type and related attributes
        $operationRequest->fk_operation_request_type_id = $request['operation_request_type_id'] ?? null;
        $operationRequest->fk_operation_request_customer_case_id = null;
        $operationRequest->fk_customer_id = $request['customer-id'] ?? null;
        $amount = $request['amount'] * 100000;
        // Ensure the amount is positive, accounting for the operation type
        $operationRequest->amount = $request['operation_type'] == 1 ? abs($amount) : -abs($amount);
        $operationRequest->description = $request['operation_desc'] ?? null;
        $operationRequest->last_change = Carbon::now();
        $operationRequest->save();

        // Return the updated OperationRequest instance
        return $operationRequest;
    }
}
