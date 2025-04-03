<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;

class EcpDailyFinancial extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'mail:ecp-daily-financial';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Automatic Daily Finance Request | ECP Settlement';

    /**
     * Create a new command instance.
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        app()->make(\App\Jobs\ECPSettlementSendExportJob::class)->dispatch();

        echo 'ECP Daily Financial Mail sent!';
    }
}
