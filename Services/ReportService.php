<?php

declare(strict_types=1);

namespace App\Service;

use Carbon\CarbonImmutable;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Filesystem\Filesystem;
use Illuminate\Database\DatabaseManager;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Collection;
use OpenSpout\Common\Entity\Cell;
use OpenSpout\Common\Entity\Row;
use OpenSpout\Writer\Common\Entity\Sheet;
use OpenSpout\Writer\XLSX\Options;
use OpenSpout\Writer\XLSX\Writer;
use Psr\Log\LoggerInterface;

final class ReportService
{
    private const int MAX_AMOUNT_OF_ROWS = 1048576;

    private const int CHUNK_SIZE = 1000;

    private int $reportIndex = 1;

    /**
     * @var array<string, bool>
     */
    private array $payeesSheetAccountIdsAlreadyProcessed = [];

    /**
     * @var array<string, bool>
     */
    private array $payeeAddressesSheetAccountIdsAlreadyProcessed = [];

    /**
     * @var array<string, bool>
     */
    private array $payeeRepresentativesSheetAccountIdsAlreadyProcessed = [];

    /**
     * @var array<string, bool>
     */
    private array $taxIdentifiersSheetAccountIdsAlreadyProcessed = [];

    /**
     * @var array<string>
     */
    private array $pathsToGeneratedFiles = [];

    private Writer $writer;

    public function __construct(
        private readonly DatabaseManager $databaseManager,
        private readonly LoggerInterface $logger,
        private readonly Repository $configRepository,
    ) {}

    /**
     * @param  array<Database>  $selectedDatabases
     * @return array<string> An array of paths
     */
    public function generate(
        int $minimumTransactionAmount,
        CarbonImmutable $trimesterStartDate,
        CarbonImmutable $trimesterEndDate,
        Filesystem $tempFilesDisk,
        string $filenamePrefix,
        array $selectedDatabases
    ): array {
        $this->createWriter($tempFilesDisk, $filenamePrefix, $selectedDatabases);

        if (in_array(Database::Paynovate, $selectedDatabases, true)) {
            $customerIds = $this->getAcquiringCustomerIds($minimumTransactionAmount, $trimesterEndDate, $trimesterStartDate);

            $this->logger->info('Got the list of customer ids', ['connection' => Database::Paynovate->value, 'count' => count($customerIds)]);

            foreach ($customerIds as $customerId) {
                $acquiringQueryBuilder = $this->acquiringQueryBuilder($trimesterStartDate, $trimesterEndDate, $customerId);

                $this->logger->info('Starting database query chunks', ['connection' => Database::Paynovate->value, 'customerId' => $customerId]);

                $amountOfRecordsProcessed = 0;
                $acquiringQueryBuilder->chunkById(
                    self::CHUNK_SIZE,
                    function (Collection $transactions) use (&$amountOfRecordsProcessed, $selectedDatabases, $tempFilesDisk, $filenamePrefix): bool {
                        $this->populateSheetData($transactions, $tempFilesDisk, $filenamePrefix, $selectedDatabases);

                        $amountOfRecordsProcessed += $transactions->count();
                        $this->logger->debug('Chunk processed', ['chunkSize' => self::CHUNK_SIZE, 'totalAmountProcessed' => $amountOfRecordsProcessed]);

                        return true;
                    },
                    't.transaction_id',
                    'transaction_id'
                );
            }
        }

        if (in_array(Database::GPS, $selectedDatabases, true)) {
            $issuingQueryBuilder = $this->issuingQueryBuilder($trimesterStartDate, $trimesterEndDate, $minimumTransactionAmount);

            $this->logger->info('Starting database query chunks', ['connection' => Database::GPS->value]);

            $amountOfRecordsProcessed = 0;
            $issuingQueryBuilder->chunkById(
                self::CHUNK_SIZE,
                function (Collection $transactions) use (&$amountOfRecordsProcessed, $selectedDatabases, &$pathsToGeneratedFiles, $tempFilesDisk, $filenamePrefix): bool {
                    $this->populateSheetData($transactions, $tempFilesDisk, $filenamePrefix, $selectedDatabases);

                    $amountOfRecordsProcessed += $transactions->count();
                    $this->logger->info('Chunk processed', ['chunkSize' => self::CHUNK_SIZE, 'totalAmountProcessed' => $amountOfRecordsProcessed]);

                    return true;
                },
                'CARDFINANCIAL_ID'
            );
        }

        if (in_array(Database::BaaS, $selectedDatabases, true)) {
            $baasPayeeQueryBuilder = $this->baasPayeeQueryBuilder($trimesterStartDate, $trimesterEndDate, $minimumTransactionAmount);

            $this->logger->info('Starting database query chunks', ['connection' => Database::BaaS->value, 'variant' => 'payee']);

            $amountOfRecordsProcessed = 0;
            $baasPayeeQueryBuilder->chunkById(
                self::CHUNK_SIZE,
                function (Collection $transactions) use (&$amountOfRecordsProcessed, $selectedDatabases, &$pathsToGeneratedFiles, $tempFilesDisk, $filenamePrefix): bool {
                    $this->populateSheetData($transactions, $tempFilesDisk, $filenamePrefix, $selectedDatabases);

                    $amountOfRecordsProcessed += $transactions->count();
                    $this->logger->info('Chunk processed', ['chunkSize' => self::CHUNK_SIZE, 'totalAmountProcessed' => $amountOfRecordsProcessed]);

                    return true;
                }
            );

            $baasPayerQueryBuilder = $this->baasPayerQueryBuilder($trimesterStartDate, $trimesterEndDate, $minimumTransactionAmount);

            $this->logger->info('Starting database query chunks', ['connection' => Database::BaaS->value, 'variant' => 'payer']);

            $amountOfRecordsProcessed = 0;
            $baasPayerQueryBuilder->chunkById(
                self::CHUNK_SIZE,
                function (Collection $transactions) use (&$amountOfRecordsProcessed, $selectedDatabases, &$pathsToGeneratedFiles, $tempFilesDisk, $filenamePrefix): bool {
                    $this->populateSheetData($transactions, $tempFilesDisk, $filenamePrefix, $selectedDatabases);

                    $amountOfRecordsProcessed += $transactions->count();
                    $this->logger->info('Chunk processed', ['chunkSize' => self::CHUNK_SIZE, 'totalAmountProcessed' => $amountOfRecordsProcessed]);

                    return true;
                }
            );
        }

        $this->writer->close();

        return $this->pathsToGeneratedFiles;
    }

    /**
     * @param  array<Database>  $selectedDatabases
     */
    private function generateTempFilepathToWriteTo(Filesystem $tempFilesDisk, string $filenamePrefix, array $selectedDatabases): string
    {
        $path = $tempFilesDisk->path(
            sprintf(
                '%s_%s_%d.xlsx',
                $filenamePrefix,
                implode('_', array_map(fn (Database $database): string => $database->value, $selectedDatabases)),
                $this->reportIndex
            )
        );

        ++$this->reportIndex;

        return $path;
    }

    private function createWriter(Filesystem $tempFilesDisk, string $filenamePrefix, array $selectedDatabases): void
    {
        $pathToWriteTo = $this->generateTempFilepathToWriteTo($tempFilesDisk, $filenamePrefix, $selectedDatabases);
        $this->pathsToGeneratedFiles[] = $pathToWriteTo;

        $this->logger->info('Creating new writer', ['filepath' => $pathToWriteTo]);

        $options = new Options();
        $options->setTempFolder(dirname($pathToWriteTo));

        $this->writer = new Writer($options);
        $this->writer->openToFile($pathToWriteTo);

        $payeesSheet = $this->writer->getCurrentSheet();
        $payeesSheet->setName(ReportSheet::Payees->value);

        $this->writer->addRow(new Row([
            Cell::fromValue('Account ID *'),
            Cell::fromValue('Account number **'),
            Cell::fromValue('Account number type **'),
            Cell::fromValue('Country of residence **'),
            Cell::fromValue('Payee names *'),
            Cell::fromValue('Country of origin *'),
            Cell::fromValue('Emails'),
            Cell::fromValue('Web pages'),
            Cell::fromValue('Reportable country *'),
        ]));

        $payeeAddressesSheet = $this->writer->addNewSheetAndMakeItCurrent();
        $payeeAddressesSheet->setName(ReportSheet::PayeeAddresses->value);

        $this->writer->addRow(new Row([
            Cell::fromValue('Account ID *'),
            Cell::fromValue('Payee address **'),
            Cell::fromValue('Country code'),
            Cell::fromValue('Legal address type'),
        ]));

        $payeeRepresentativesSheet = $this->writer->addNewSheetAndMakeItCurrent();
        $payeeRepresentativesSheet->setName(ReportSheet::PayeeRepresentatives->value);

        $this->writer->addRow(new Row([
            Cell::fromValue('Account ID *'),
            Cell::fromValue('Representative ID *'),
            Cell::fromValue('Type of representative *'),
            Cell::fromValue('Representative names'),
        ]));

        $taxIdentifiersSheet = $this->writer->addNewSheetAndMakeItCurrent();
        $taxIdentifiersSheet->setName(ReportSheet::TaxIdentifiers->value);

        $this->writer->addRow(new Row([
            Cell::fromValue('Account ID *'),
            Cell::fromValue('Tax identifier **'),
            Cell::fromValue('Issuer country **'),
            Cell::fromValue('Identifier type **'),
        ]));

        $paymentsToPayeesSheet = $this->writer->addNewSheetAndMakeItCurrent();
        $paymentsToPayeesSheet->setName(ReportSheet::PaymentsToPayees->value);

        $this->writer->addRow(new Row([
            Cell::fromValue('Account ID *'),
            Cell::fromValue('Refund'),
            Cell::fromValue('Payment ID *'),
            Cell::fromValue('Refunded payment ID **'),
            Cell::fromValue('Date-time of payment *'),
            Cell::fromValue('Payment date type *'),
            Cell::fromValue('Payment amount *'),
            Cell::fromValue('Payment currency *'),
            Cell::fromValue('Initiated on merchant premises *'),
            Cell::fromValue('Country code of payer *'),
            Cell::fromValue('Payer location identifier *'),
            Cell::fromValue('Payment method type'),
            Cell::fromValue('Other payment method description **'),
            Cell::fromValue('Payment service provider type'),
            Cell::fromValue('Other payment service provider description **'),
        ]));
    }

    private function acquiringQueryBuilder(
        CarbonImmutable $trimesterStartDate,
        CarbonImmutable $trimesterEndDate,
        int $customerId,
    ): Builder {
        $countries = ['Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic', 'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands', 'Poland', 'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden'];
        $isoAlpha3Array = $this->configRepository->get('iso_alpha3_to_alpha2');
        $caseStatement = 'CASE';
        foreach ($isoAlpha3Array as $alpha3 => $alpha2) {
            $caseStatement .= sprintf(" WHEN c.country_code = '%s' THEN '%s'", $alpha3, $alpha2);
        }

        $caseStatement .= ' ELSE c.country_code END';

        $isoCurrencyArray = $this->configRepository->get('iso_currencies');
        $caseCurrency = 'CASE';
        foreach ($isoCurrencyArray as $code => $currency) {
            $caseCurrency .= sprintf(" WHEN right(t.Merchant_Service_Charge_Currency, 3) = '%s' THEN '%s'", $code, $currency);
        }

        $caseCurrency .= ' ELSE t.Merchant_Service_Charge_Currency END';

        $isoCountryArray = $this->configRepository->get('iso_countries_to_alpha2');
        $caseCountry = 'CASE';
        foreach ($isoCountryArray as $alpha2 => $country) {
            // Escape special characters that need to be escaped in SQL
            $escapedCountry = addslashes((string) $country);
            $caseCountry .= sprintf(" WHEN t.cardholder_country = '%s' THEN '%s'", $escapedCountry, $alpha2);
        }

        $caseCountry .= ' ELSE t.cardholder_country END';

        return $this->databaseManager->connection(Database::Paynovate->value)
            ->query()
            ->selectRaw('CONV(c.VAT, 36, 10) AS Account_ID')
            ->addSelect('t.transaction_id AS transaction_id')
            ->addSelect('c.iban AS Account_number')
            ->selectRaw("'IBAN' AS Account_number_type")
            ->addSelect($this->databaseManager->raw($caseStatement.' AS Country_of_residence'))
            ->selectRaw("concat(c.legal_name, ', LEGAL') AS Payee_names")
            ->addSelect($this->databaseManager->raw($caseStatement.' AS Country_of_origin'))
            ->selectRaw("' ' AS Emails")
            ->selectRaw("' ' AS Web_Pages")
            ->addSelect($this->databaseManager->raw($caseStatement.' AS Reported_country'))
            ->selectRaw('concat(c.address1, c.zip, c.locality) AS Payee_addresses')
            ->addSelect($this->databaseManager->raw($caseStatement.' AS Country_code'))
            ->selectRaw("'RESIDENTIAL_OR_BUSINESS' AS Legal_address_type")
            ->selectRaw("' ' AS Representative_ID")
            ->selectRaw("' ' AS Type_of_representative")
            ->selectRaw("' ' AS Representative_names")
            ->addSelect('c.VAT AS Tax_identifier')
            ->selectRaw('left(c.VAT, 2) AS Issuer_country')
            ->selectRaw("'VAT' AS Identifier_type")
            ->selectRaw("'FALSE' AS Refund")
            ->addSelect('t.transaction_id AS Payment_ID')
            ->selectRaw('ROUND(t.raw_amount/100000, 2) AS Payment_amount')
            ->selectRaw("' ' AS Refunded_payment_ID")
            ->addSelect($this->databaseManager->raw("DATE_FORMAT(t.creation_date, '%Y-%m-%dT%H:%i:%s.000+00:00') AS `Date_time_of_payment`"))
            ->selectRaw("'CLEARING' AS Payment_date_type")
            ->addSelect($this->databaseManager->raw($caseCurrency.' AS Payment_currency'))
            ->selectRaw("if (t.pos_entry_mode != '00', 'TRUE', 'FALSE') AS Initiated_on_merchant_premise")
            ->addSelect($this->databaseManager->raw($caseCountry.' AS Country_code_of_payer'))
            ->selectRaw("'OTHER' AS Payer_location_identifier")
            ->selectRaw("'CARD' AS Payment_method_type")
            ->selectRaw("' ' AS Other_payment_method_description")
            ->selectRaw(
                "if (t.fk_transaction_source_id in ('12', '42'), 'ACQUIRER', 'PAYMENT_PROCESSOR') AS Payment_service_provider_type"
            )
            ->selectRaw("' ' AS Other_payment_service_provider_description")
            ->fromRaw('transaction AS t FORCE INDEX (ix_customer_date)')
            ->join('customer AS c', 't.fk_customer_id', '=', 'c.customer_id')
            ->whereBetween('t.date', [$trimesterStartDate->format('Y-m-d'), $trimesterEndDate->format('Y-m-d')])
            ->whereIn('t.cardholder_country', $countries)
            ->where('t.interchange_region', '=', 'Intra-region')
            ->whereIn('t.service', ['8810', '8811', '8843', '8010'])
            ->whereNot(function (Builder $query): void {
                $query->where('c.country_code', '=', 'BEL')->where('t.fk_card_brand_id', '=', 32);
            })
            ->where('c.customer_id', $customerId);
    }

    private function issuingQueryBuilder(
        CarbonImmutable $trimesterStartDate,
        CarbonImmutable $trimesterEndDate,
        int $minimumTransactionAmount,
    ): Builder {
        $termCountries = ['AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE', 'EU'];
        $cardProductIds = [3989, 3990, 3991, 4017, 4018, 4019, 4295, 2152, 2156, 2157, 2166, 2152, 2156, 2157, 2166, 813, 815, 817, 847, 851, 852, 853, 854, 855, 857, 858, 861, 864, 865, 867, 870, 874, 876, 877, 2228, 2232, 2284, 2311, 2312, 1313, 6238, 8475, 15640, 15656, 15657, 15659, 15660, 15661, 15710, 15726, 15727, 15729, 15730, 15731, 15683, 15684, 15686, 15687, 15688, 15689, 15746, 15770, 15771, 15773, 15774, 15775, 9949, 9965, 15877, 15879, 15881, 16041, 16046, 16047, 16048, 16055, 16056, 16060, 16080, 16088, 16098, 16099, 16111, 16132, 16133, 16145, 16155, 16159, 16166, 16168, 6295, 6296, 7150, 7152, 8195, 9676, 9677, 9678, 9679, 9680, 6297, 6298, 7153, 7154, 8668, 9697, 9698, 9699, 9700, 9701, 12234, 12235, 12236, 12237, 12238, 12239, 12240, 12480, 12481, 12482, 12483, 12484, 12485, 12486, 12487, 12488, 12489, 12490, 12491, 14768, 14770, 14771, 14772, 14773, 14774, 14775, 14776, 14777, 14778, 14779, 14780, 1639, 2702, 2778, 3256, 3257, 3258, 3259, 4207, 4208, 4209, 4210, 4211, 4212, 4213, 4214, 12513, 12514, 12515, 12516, 12517, 12518, 12519, 12520, 17149, 17167, 17168, 17169, 17170, 17171, 17326, 17327, 17328, 17329, 17330, 17333, 17334, 17335, 17336, 17337, 17338, 17339, 17340, 17341, 17342, 17343, 17344, 17345, 17346, 17347, 17348, 17349, 17350, 17351, 17352, 17353, 17354, 17355, 2167, 2168, 2169, 2170, 1602, 1687, 1691, 1733, 1749, 1813, 1887, 1888, 1889, 1996, 1997, 1998, 1999, 2025, 2026, 2153, 2154, 2155, 2164, 3283, 3352, 3859, 16441, 980, 981, 982, 983, 984, 985, 986, 987, 988, 989, 990, 991, 992, 993, 994, 995, 996, 997, 998, 999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1023, 1024, 1025, 1511, 1512, 2230, 2231, 2233, 2285, 2314, 2315, 2316, 2410, 2411, 2412, 2413, 2414, 2415, 2145, 2146, 3000, 3434, 3435, 3436, 3437, 1734];

        $isoCurrencyArray = $this->configRepository->get('iso_currencies');
        $caseCurrency = 'CASE';
        foreach ($isoCurrencyArray as $code => $currency) {
            $caseCurrency .= sprintf(" WHEN cf.TXNAMT_CURRENCY = '%s' THEN '%s'", $code, $currency);
        }

        $caseCurrency .= ' ELSE cf.TXNAMT_CURRENCY END';

        $isoProductArray = $this->configRepository->get('iso_alpha2_countries_from_product_card_id');
        $caseAlpha2 = 'CASE';
        foreach ($isoProductArray as $code => $alpha2) {
            $caseAlpha2 .= sprintf(" WHEN cf.CARD_PRODUCTID = '%s' THEN '%s'", $code, $alpha2);
        }

        $caseAlpha2 .= ' ELSE cf.CARD_PRODUCTID END';

        return $this->databaseManager->connection(Database::GPS->value)
            ->query()
            ->addSelect('CARDFINANCIAL_ID')
            ->selectRaw('CONV(cf.term_location, 36, 10) AS Account_ID')
            ->addSelect('cf.MERCHCODE AS Account_number')
            ->selectRaw("' ' AS Account_number_of_payees")
            ->selectRaw("'OTHER' AS Account_number_type")
            ->selectRaw("' ' AS Country_of_residence")
            ->selectRaw("CONCAT(cf.term_location, ', OTHER') AS Payee_names")
            ->addSelect('cf.TERM_COUNTRY AS Country_of_origin')
            ->selectRaw("' ' AS Emails")
            ->selectRaw("' ' AS Web_Pages")
            ->selectRaw("'BE' AS Reported_country")
            ->selectRaw("' ' AS Payee_addresses")
            ->selectRaw("' ' AS Country_code")
            ->selectRaw("' ' AS Legal_address_type")
            ->selectRaw("' ' AS Representative_ID")
            ->selectRaw("' ' AS Type_of_representative")
            ->selectRaw("' ' AS Representative_names")
            ->selectRaw("' ' AS Tax_identifier")
            ->selectRaw("' ' AS Issuer_country")
            ->selectRaw("' ' AS Identifier_type")
            ->selectRaw("'FALSE' AS Refund")
            ->addSelect('cf.arn AS Payment_ID')
            ->selectRaw("' ' AS Refunded_payment_ID")
            ->addSelect($this->databaseManager->raw("DATE_FORMAT(cf.settlementdate, '%Y-%m-%dT%H:%i:%s.000+00:00') AS Date_time_of_payment"))
            ->selectRaw("'CLEARING' AS Payment_date_type")
            ->selectRaw('ROUND(cf.TXNAMT_VALUE, 2) AS Payment_amount')
            ->addSelect('cf.TXNAMT_CURRENCY  AS Payment_currency')
            ->addSelect($this->databaseManager->raw($caseCurrency.' AS Payment_currency'))
            ->selectRaw("IF(cf.TXN_CARDHOLDERPRESENT = '0', 'TRUE', 'FALSE') AS Initiated_on_merchant_premise")
            ->addSelect($this->databaseManager->raw($caseAlpha2.' AS Country_code_of_payer'))
            ->selectRaw("'OTHER' AS Payer_location_identifier")
            ->selectRaw("'CARD' AS Payment_method_type")
            ->selectRaw("' ' AS Other_payment_method_description")
            ->selectRaw("'ISSUER' AS Payment_service_provider_type")
            ->selectRaw("' ' AS Other_payment_service_provider_description")
            ->from('CARDFINANCIAL as cf')
            ->whereBetween('cf.SETTLEMENTDATE', [$trimesterStartDate->format('Ymd'), $trimesterEndDate->format('Ymd')])
            ->whereNotIn('cf.TERM_COUNTRY', $termCountries)
            ->whereNotIn('cf.CARD_PRODUCTID', $cardProductIds)
            ->where('cf.TXNCODE_TYPE', 'pos')
            ->whereIn(
                'cf.term_location',
                function ($query) use ($trimesterStartDate, $trimesterEndDate, $minimumTransactionAmount): void {
                    $query->from('CARDFINANCIAL')
                        ->select('term_location')
                        ->whereBetween('SETTLEMENTDATE', [$trimesterStartDate->format('Ymd'), $trimesterEndDate->format('Ymd')])
                        ->groupBy('term_location')
                        ->havingRaw('COUNT(*) > ?', [$minimumTransactionAmount]);
                }
            );
    }

    private function baasPayeeQueryBuilder(
        CarbonImmutable $trimesterStartDate,
        CarbonImmutable $trimesterEndDate,
        int $minimumTransactionAmount,
    ): Builder {
        $euMemberStatesAlpha2 = ['AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE'];

        return $this->databaseManager->connection(Database::BaaS->value)
            ->query()
            ->addSelect('id')
            ->selectRaw('CONV(`cr_acc`, 36, 10) AS Account_ID')
            ->addSelect('cr_acc AS Account_number')
            ->selectRaw("'IBAN' AS Account_number_type")
            ->addSelect('cr_address_country AS Country_of_residence')
            ->selectRaw("CONCAT(cr_name, ', OTHER') AS Payee_names")
            ->addSelect('cr_address_country AS Country_of_origin')
            ->selectRaw("' ' AS Emails")
            ->selectRaw("' ' AS Web_Pages")
            ->addSelect('cr_address_country AS Reported_country')
            ->selectRaw("CONCAT(cr_address, ', ', cr_address_city) AS Payee_addresses")
            ->addSelect('cr_address_country AS Country_code')
            ->selectRaw("'RESIDENTIAL_OR_BUSINESS' AS Legal_address_type")
            ->selectRaw("' ' AS Representative_ID")
            ->selectRaw("' ' AS Type_of_representative")
            ->selectRaw("' ' AS Representative_names")
            ->selectRaw("' ' AS Tax_identifier")
            ->selectRaw("' ' AS Issuer_country")
            ->selectRaw("' ' AS Identifier_type")
            ->selectRaw("'FALSE' AS Refund")
            ->addSelect('id AS Payment_ID')
            ->selectRaw("' ' AS Refunded_payment_ID")
            ->addSelect($this->databaseManager->raw("DATE_FORMAT(settlement_date, '%Y-%m-%dT%H:%i:%s.000+00:00') AS Date_time_of_payment"))
            ->selectRaw("'EXECUTION' AS Payment_date_type")
            ->selectRaw('ROUND(`cr_amount`/100, 2) AS Payment_amount')
            ->addSelect('cr_ccy_isocode AS Payment_currency')
            ->selectRaw("'FALSE' AS Initiated_on_merchant_premise")
            ->addSelect('dr_address_country AS Country_code_of_payer')
            ->selectRaw("'OTHER' AS Payer_location_identifier")
            ->selectRaw("IF(method_id IN ('5', '6'), 'DIRECT_DEBIT', 'BANK_TRANSFER') AS Payment_method_type")
            ->selectRaw("' ' AS Other_payment_method_description")
            ->selectRaw("'ISSUER' AS Payment_service_provider_type")
            ->selectRaw("' ' AS Other_payment_service_provider_description")
            ->from($this->databaseManager->raw('(SELECT @row := 0) AS r, transactions'))
            ->whereRaw('`dr_address_country` != `cr_address_country`')
            ->where('cr_bank_bic', 'like', 'PAYVBE%')
            ->whereIn('cr_address_country', $euMemberStatesAlpha2)
            ->whereIn('dr_address_country', $euMemberStatesAlpha2)
            ->whereBetween('settlement_date', [$trimesterStartDate, $trimesterEndDate])
            ->whereIn('cr_acc', function (Builder $query) use (
                $minimumTransactionAmount
            ): void {
                $query->select('cr_acc')
                    ->from('transactions')
                    ->groupBy('cr_acc')
                    ->havingRaw('COUNT(cr_acc) > ?', [$minimumTransactionAmount]);
            });
    }

    private function baasPayerQueryBuilder(
        CarbonImmutable $trimesterStartDate,
        CarbonImmutable $trimesterEndDate,
        int $minimumTransactionAmount,
    ): Builder {
        $euMemberStatesAlpha2 = ['AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE'];

        return $this->databaseManager->connection(Database::BaaS->value)
            ->query()
            ->addSelect('id')
            ->selectRaw('CONV(`cr_acc`, 36, 10) AS Account_ID')
            ->addSelect('cr_acc as Account_number')
            ->selectRaw("'IBAN' as Account_number_type")
            ->addSelect('cr_address_country as Country_of_residence')
            ->selectRaw("CONCAT(cr_name, ', OTHER') as Payee_names")
            ->addSelect('cr_address_country as Country_of_origin')
            ->selectRaw("' ' AS Emails")
            ->selectRaw("' ' AS Web_Pages")
            ->selectRaw("'BE' as Reported_Country")
            ->selectRaw("CONCAT(cr_address, ', ', cr_address_city) as Payee_addresses")
            ->addSelect('cr_address_country as Country_code')
            ->selectRaw("'RESIDENTIAL_OR_BUSINESS' as Legal_address_type")
            ->selectRaw("' ' as Representative_ID")
            ->selectRaw("' ' as Type_of_representative")
            ->selectRaw("' ' as Representative_names")
            ->selectRaw("' ' as Tax_identifier")
            ->selectRaw("' ' as Issuer_country")
            ->selectRaw("' ' as Identifier_type")
            ->selectRaw("'FALSE' as Refund")
            ->addSelect('id as Payment_ID')
            ->selectRaw("' ' as Refunded_payment_ID")
            ->addSelect($this->databaseManager->raw("DATE_FORMAT(settlement_date, '%Y-%m-%dT%H:%i:%s.000+00:00') AS Date_time_of_payment"))
            ->selectRaw("'EXECUTION' as Payment_date_type")
            ->selectRaw('ROUND(`dr_amount`/100, 2) as Payment_amount')
            ->addSelect('dr_ccy_isocode as Payment_currency')
            ->selectRaw("'FALSE' as Initiated_on_merchant_premise")
            ->addSelect('dr_address_country as Country_code_of_payer')
            ->selectRaw("'OTHER' as Payer_location_identifier")
            ->selectRaw("IF(method_id IN ('5', '6'), 'DIRECT_DEBIT', 'BANK_TRANSFER') as Payment_method_type")
            ->selectRaw("' ' AS Other_payment_method_description")
            ->selectRaw("'ISSUER' as Payment_service_provider_type")
            ->selectRaw("' ' as Other_payment_service_provider_description")
            ->from($this->databaseManager->raw('(SELECT @row := 0) AS r, transactions'))
            ->whereRaw('`dr_address_country` != `cr_address_country`')
            ->where('dr_bank_bic', 'like', 'PAYVBE%')
            ->whereIn('cr_address_country', $euMemberStatesAlpha2)
            ->whereIn('dr_address_country', $euMemberStatesAlpha2)
            ->whereBetween('settlement_date', [$trimesterStartDate, $trimesterEndDate])
            ->whereIn('cr_acc', function (Builder $query) use (
                $minimumTransactionAmount
            ): void {
                $query->select('cr_acc')
                    ->from('transactions')
                    ->groupBy('cr_acc')
                    ->havingRaw('COUNT(cr_acc) > ?', [$minimumTransactionAmount]);
            });
    }

    private function isMaximumAmountOfRowsReachedForAnySheet(): bool
    {
        return array_reduce(
            [
                $this->getSheetFromWriter(ReportSheet::Payees)->getWrittenRowCount(),
                $this->getSheetFromWriter(ReportSheet::PayeeAddresses)->getWrittenRowCount(),
                $this->getSheetFromWriter(ReportSheet::PayeeRepresentatives)->getWrittenRowCount(),
                $this->getSheetFromWriter(ReportSheet::TaxIdentifiers)->getWrittenRowCount(),
                $this->getSheetFromWriter(ReportSheet::PaymentsToPayees)->getWrittenRowCount(),
            ],
            fn (bool $carry, int $count): bool => $carry || $count >= self::MAX_AMOUNT_OF_ROWS,
            false
        );
    }

    private function getSheetFromWriter(ReportSheet $sheetName): Sheet
    {
        $sheets = $this->writer->getSheets();

        return current(array_filter($sheets, fn (Sheet $sheet): bool => $sheet->getName() === $sheetName->value));
    }

    private function populateSheetData(Collection $transactions, Filesystem $tempFilesDisk, string $filenamePrefix, array $selectedDatabases): void
    {
        $this->logger->info('Amount of transactions', ['amount' => $transactions->count()]);

        foreach ($transactions as $transaction) {
            // We check if we reached the maximum amount of rows an Excel sheet can handle
            // If we're there, close the current writer, and create a new writer with a different path to write to
            if ($this->isMaximumAmountOfRowsReachedForAnySheet()) {
                $this->writer->close();

                $this->logger->info('Maximum amount of rows reached. Closed current writer.');

                $this->createWriter($tempFilesDisk, $filenamePrefix, $selectedDatabases);
            }

            $this->addSheetsToReport($transaction);
        }
    }

    private function addSheetsToReport(\stdClass $transaction): void
    {
        $this->logger->debug('Current record being processed', ['Account_ID' => $transaction->Account_ID]);

        if (! array_key_exists($transaction->Account_ID, $this->payeesSheetAccountIdsAlreadyProcessed)) {
            $this->payeesSheetAccountIdsAlreadyProcessed[$transaction->Account_ID] = true;

            $this->logger->debug('Adding account to Payees sheet', ['Account_ID' => $transaction->Account_ID]);

            $this->writer->setCurrentSheet($this->getSheetFromWriter(ReportSheet::Payees));
            $this->writer->addRow(
                Row::fromValues(
                    [
                        $transaction->Account_ID,
                        $transaction->Account_number,
                        $transaction->Account_number_type,
                        $transaction->Country_of_residence,
                        $transaction->Payee_names,
                        $transaction->Country_of_origin,
                        $transaction->Emails,
                        $transaction->Web_Pages,
                        $transaction->Reported_country,
                    ]
                )
            );
        }

        if (! array_key_exists($transaction->Account_ID, $this->payeeAddressesSheetAccountIdsAlreadyProcessed)) {
            $this->payeeAddressesSheetAccountIdsAlreadyProcessed[$transaction->Account_ID] = true;

            $this->logger->debug('Adding account to Payee addresses sheet (if non-empty fields)', ['Account_ID' => $transaction->Account_ID]);

            if (! is_null($transaction->Payee_addresses) && trim($transaction->Payee_addresses) !== '' && trim($transaction->Payee_addresses) !== '0' && (! is_null($transaction->Country_code) && trim($transaction->Country_code) !== '' && trim($transaction->Country_code) !== '0') && (! is_null($transaction->Legal_address_type) && trim($transaction->Legal_address_type) !== '' && trim($transaction->Legal_address_type) !== '0')) {
                $this->writer->setCurrentSheet($this->getSheetFromWriter(ReportSheet::PayeeAddresses));
                $this->writer->addRow(
                    Row::fromValues(
                        [
                            $transaction->Account_ID,
                            $transaction->Payee_addresses,
                            $transaction->Country_code,
                            $transaction->Legal_address_type,
                        ]
                    )
                );
            }
        }

        if (! array_key_exists($transaction->Account_ID, $this->payeeRepresentativesSheetAccountIdsAlreadyProcessed)) {
            $this->payeeRepresentativesSheetAccountIdsAlreadyProcessed[$transaction->Account_ID] = true;

            $this->logger->debug('Adding account to Payee representatives sheet (if non-empty fields)', ['Account_ID' => $transaction->Account_ID]);

            if (! is_null($transaction->Representative_ID) && trim($transaction->Representative_ID) !== '' && trim($transaction->Representative_ID) !== '0' && (! is_null($transaction->Type_of_representative) && trim($transaction->Type_of_representative) !== '' && trim($transaction->Type_of_representative) !== '0') && (! is_null($transaction->Representative_names) && trim($transaction->Representative_names) !== '' && trim($transaction->Representative_names) !== '0')) {
                $this->writer->setCurrentSheet($this->getSheetFromWriter(ReportSheet::PayeeRepresentatives));
                $this->writer->addRow(
                    Row::fromValues(
                        [
                            $transaction->Account_ID,
                            $transaction->Representative_ID,
                            $transaction->Type_of_representative,
                            $transaction->Representative_names,
                        ]
                    )
                );
            }
        }

        if (! array_key_exists($transaction->Account_ID, $this->taxIdentifiersSheetAccountIdsAlreadyProcessed)) {
            $this->taxIdentifiersSheetAccountIdsAlreadyProcessed[$transaction->Account_ID] = true;

            $this->logger->debug('Adding account to Tax identifiers sheet (if non-empty fields)', ['Account_ID' => $transaction->Account_ID]);

            if (! is_null($transaction->Tax_identifier) && trim($transaction->Tax_identifier) !== '' && trim($transaction->Tax_identifier) !== '0' && (! is_null($transaction->Issuer_country) && trim($transaction->Issuer_country) !== '' && trim($transaction->Issuer_country) !== '0') && (! is_null($transaction->Identifier_type) && trim($transaction->Identifier_type) !== '' && trim($transaction->Identifier_type) !== '0')) {
                $this->writer->setCurrentSheet($this->getSheetFromWriter(ReportSheet::TaxIdentifiers));
                $this->writer->addRow(
                    Row::fromValues(
                        [
                            $transaction->Account_ID,
                            $transaction->Tax_identifier,
                            $transaction->Issuer_country,
                            $transaction->Identifier_type,
                        ]
                    )
                );
            }
        }

        $this->writer->setCurrentSheet($this->getSheetFromWriter(ReportSheet::PaymentsToPayees));
        $this->writer->addRow(
            Row::fromValues(
                [
                    $transaction->Account_ID,
                    $transaction->Refund,
                    $transaction->Payment_ID,
                    $transaction->Refunded_payment_ID,
                    $transaction->Date_time_of_payment,
                    $transaction->Payment_date_type,
                    $transaction->Payment_amount,
                    $transaction->Payment_currency,
                    $transaction->Initiated_on_merchant_premise,
                    $transaction->Country_code_of_payer,
                    $transaction->Payer_location_identifier,
                    $transaction->Payment_method_type,
                    $transaction->Other_payment_method_description,
                    $transaction->Payment_service_provider_type,
                    $transaction->Other_payment_service_provider_description,
                ]
            )
        );
    }

    /**
     * @return int[]
     */
    public function getAcquiringCustomerIds(
        int $minimumTransactionAmount,
        CarbonImmutable $trimesterEndDate,
        CarbonImmutable $trimesterStartDate
    ): array {
        return $this->databaseManager->connection(Database::Paynovate->value)
            ->query()
            ->select('c.customer_id')
            ->fromRaw('customer AS c USE INDEX (country_code)')
            ->whereIn(
                'c.customer_id',
                function (Builder $query) use (
                    $minimumTransactionAmount,
                    $trimesterEndDate,
                    $trimesterStartDate
                ): void {
                    $query->select('fk_customer_id')
                        ->from('customer_stats')
                        ->whereNot('fk_customer_id', '=', 0)
                        ->where(
                            'year',
                            $trimesterStartDate->format('Y')
                        )
                        ->whereBetween(
                            'month',
                            [$trimesterStartDate->format('m'), $trimesterEndDate->format('m')]
                        )
                        ->groupBy('fk_customer_id')
                        ->havingRaw('SUM(transaction_count) > ?', [$minimumTransactionAmount]);
                }
            )
            ->whereIn(
                'c.country_code',
                [
                    'AUT',
                    'BEL',
                    'BGR',
                    'HRV',
                    'CYP',
                    'CZE',
                    'DNK',
                    'EST',
                    'FIN',
                    'FRA',
                    'DEU',
                    'GRC',
                    'HUN',
                    'IRL',
                    'ITA',
                    'LVA',
                    'LTU',
                    'LUX',
                    'MLT',
                    'NLD',
                    'POL',
                    'PRT',
                    'ROU',
                    'SVK',
                    'SVN',
                    'ESP',
                    'SWE',
                ]
            )
            ->pluck('c.customer_id')
            ->toArray();
    }
}
