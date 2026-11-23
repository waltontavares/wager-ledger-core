import {
  CanActivate,
  ExecutionContext,
  Inject,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import {
  AuthenticatedProvider,
  PROVIDER_IDENTITY_PORT,
  ProviderIdentityPort,
} from '../../domain/ports/provider-identity.port.js';

@Injectable()
export class HeaderProviderIdentityAdapter implements ProviderIdentityPort {
  async authenticate(
    headers: Record<string, string | string | undefined>,
  ): Promise<AuthenticatedProvider> {
    const providerId = headers['x-provider-id'];
    const secret = headers['x-internal-secret'];
    const expectedSecret = process.env.INTERNAL_PROVIDER_SECRET;

    if (
      typeof providerId !== 'string' ||
      providerId.trim() === '' ||
      typeof secret !== 'string' ||
      !expectedSecret ||
      secret !== expectedSecret
    ) {
      throw new UnauthorizedException('Invalid or missing provider identity credentials');
    }

    return {
      providerId: providerId.trim(),
      scopes: ['wager:write', 'wager:read'],
    };
  }
}

@Injectable()
export class ExplicitProviderIdentityGuard implements CanActivate {
  constructor(
    @Inject(PROVIDER_IDENTITY_PORT)
    private readonly identityPort: ProviderIdentityPort,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest();
    const provider = await this.identityPort.authenticate(request.headers);
    request.provider = provider;
    return true;
  }
}
