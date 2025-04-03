<?php

namespace App\Http\Controllers\Api\User;

use App\Events\UserCreatedEvent;
use App\Foundation\Helpers\UrlHelper;
use App\Http\Controllers\Api\ApiController;
use Application\Contract\InvitationLinkRepositoryInterface;
use Application\Contract\UserRepositoryInterface;
use Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Http\Response;
use Illuminate\Support\Facades\Password;
use ReflectionException;

class VerifyUserEmail extends ApiController
{
    public function __construct(private readonly InvitationLinkRepositoryInterface $invitationLinkRepository, private readonly UserRepositoryInterface $userRepository) {}

    /**
     * @OA\Post(
     * path="/api/user/resend-onboarding-email",
     * summary="Resend onboarding email",
     * description="This will resend the onboarding email to given user",
     * operationId="verify-email",
     * tags={"User"},
     *
     * @OA\RequestBody(
     *    required=true,
     *
     *    @OA\JsonContent(
     *
     *       @OA\Property(property="email", type="string"),
     *       @OA\Property(property="client_id", type="string"),
     *       @OA\Property(property="send", type="boolean")
     *   ),
     * ),
     *
     * @OA\Response(
     *    response=201,
     *    description="Created",
     * ),
     * @OA\Response(
     *    response=204,
     *    description="Success no content",
     * ),
     *  @OA\Response(
     *    response=401,
     *    description="Unauthenticated",
     *
     *    @OA\JsonContent(
     *
     *       @OA\Property(property="message", type="string", example="Unauthenticated"),
     *    )
     *  ),
     *
     * @OA\Response(
     *    response=422,
     *    description="Validation errors.",
     *
     *    @OA\JsonContent(
     *
     *       @OA\Property(property="message", type="string", example="The given data was invalid."),
     *       @OA\Property(property="errors", type="object")
     *    )
     * ),
     *
     * @OA\Response(
     *    response=500,
     *    description="Bad request.",
     *
     *    @OA\JsonContent(
     *
     *       @OA\Property(property="error", type="string"),
     *       @OA\Property(property="message", type="string"),
     *    )
     *  ),
     * )
     *)
     *
     * @return JsonResponse
     *
     * @throws ReflectionException
     */
    public function __invoke(Request $request)
    {
        $data = $request->validate([
            'client_id' => ['required', 'uuid'],
            'email' => ['required', 'email:rfc,dns'],
            'send' => ['required'],
        ]);

        try {
            $user = $this->userRepository->findByEmail($data['email']);
        } catch (ModelNotFoundException $exception) {
            return $this->error('Not found.', 'User not found.', Response::HTTP_NOT_FOUND);
        }
        if ($user->password == null || $user->password == '') {
            $url = UrlHelper::configure(config('app.url').route('password-change-authorize', [], false));
            $url->setKey($data['email']);
            $url->setClientId($data['client_id']);
            $this->invitationLinkRepository->create($user->id, $url->getSignature());

            if ($data['send']) {
                event(new UserCreatedEvent($user->name, $user->email, $data['client_id'], $this->userRepository->getAccessibleClients($user->id), $url->signedUrl()));
            }

            return $this->response(['url' => $url->signedUrl()]);
        }
        try {
            $this->broker()->getUser($request->only('email'));

            if ($this->broker()->getRepository()->recentlyCreatedToken($user)) {
                return $this->error('Too many requests.', 'Wait for some time.', Response::HTTP_TOO_MANY_REQUESTS);
            }

            $token = $this->broker()->getRepository()->create($user);
            $user->sendPasswordResetNotification($token);

            return $this->response(['url' => config('app.url').'/password/reset/'.$token.'?email='.$data['email']]);
        } catch (\Exception $exception) {
            return $this->error('Something went wrong.', $exception->getMessage());
        }
    }

    public function broker()
    {
        return Password::broker();
    }
}
