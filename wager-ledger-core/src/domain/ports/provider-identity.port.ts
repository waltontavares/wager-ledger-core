export interface AuthenticatedProvider {
  providerId: string;
  scopes: string[];
}

export const PROVIDER_IDENTITY_PORT = Symbol('PROVIDER_IDENTITY_PORT');

export interface ProviderIdentityPort {
  authenticate(headers: Record<string, string | string[] | undefined>): Promise<AuthenticatedProvider>;
}
