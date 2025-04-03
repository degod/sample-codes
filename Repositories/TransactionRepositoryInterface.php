<?php

namespace App\Repository\Transaction;

interface TransactionRepositoryInterface
{
    /**
     * @param  'ALL'|'BLOCKED'|'UNPAID'|'PAID'  $transactionStatus
     */
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
    ): object;

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
    ): object;

    public function paymentChecks(?string $fromDate = null, ?string $toDate = null): object;

    public function getPaymentAlerts(?string $fromDate = null, ?string $toDate = null, ?string $mcc = null, ?string $paid = null): array;

    /**
     * @param  null  $filter
     * @param  array|string[]  $select
     * @return mixed
     */
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
    ): object;

    /** append in deletion_date new date.
     *
     */
    public function softDeleteOpReq(string $id);

    public function store(array $request);
}
