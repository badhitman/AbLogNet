////////////////////////////////////////////////
// © https://github.com/badhitman 
////////////////////////////////////////////////

using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Text;

namespace SharedLib;

/// <summary>
/// Шифрование
/// </summary>
[SupportedOSPlatform("windows")]
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("android")]
public static class CipherService
{
    static readonly Encoding encoding = Encoding.UTF8;
    static readonly int secret_key_size = 32, iv_key_size = 16;

    /// <summary>
    /// Секретная фраза по умолчанию (если пользователь не указал свою) для шифрования
    /// </summary>
    public const string DefaultSecret = "1B1B7181-F700-461A-A8B7-4CA3E8E7B9EB";

    /// <summary>
    /// Encrypt as string
    /// </summary>
    public static async Task<string> EncryptAsStringAsync(string clearText, string EncryptionKey, byte[] salt) => Convert.ToBase64String(await EncryptAsync(encoding.GetBytes(clearText), EncryptionKey, salt));

    /// <summary>
    /// Encrypt as string
    /// </summary>
    public static async Task<string> EncryptAsStringAsync(string clearText, string EncryptionKey, string salt) => Convert.ToBase64String(await EncryptAsync(encoding.GetBytes(clearText), EncryptionKey, encoding.GetBytes(salt)));

    /// <summary>
    /// Encrypt
    /// </summary>
    public static async Task<byte[]> EncryptAsync(string clearText, string EncryptionKey, string salt) => await EncryptAsync(encoding.GetBytes(clearText), EncryptionKey, encoding.GetBytes(salt));

    /// <summary>
    /// Encrypt
    /// </summary>
    public static async Task<byte[]> EncryptAsync(byte[] clearBytes, string EncryptionKey, byte[] salt)
    {
        using Aes encryption = Aes.Create();
        using Rfc2898DeriveBytes pdb = new(EncryptionKey, salt, 3, hashAlgorithm: HashAlgorithmName.SHA256);
        encryption.Key = pdb.GetBytes(secret_key_size);
        encryption.IV = pdb.GetBytes(iv_key_size);
        using MemoryStream ms = new();
        using CryptoStream cs = new(ms, encryption.CreateEncryptor(), CryptoStreamMode.Write);
        await cs.WriteAsync(clearBytes, 0, clearBytes.Length);
        cs.Close();
        return ms.ToArray();
    }


    /// <summary>
    /// Расшифровать строку
    /// </summary>
    public static async Task<string> DecryptAsStringAsync(string cipherText, string EncryptionKey, byte[] salt) => Convert.ToBase64String(await DecryptAsync(Convert.FromBase64String(cipherText.Replace(" ", "+")), EncryptionKey, salt));

    /// <summary>
    /// Расшифровать
    /// </summary>        
    public static async Task<byte[]> DecryptAsync(byte[] cipherBytes, string EncryptionKey, byte[] salt)
    {
        using Aes encryption = Aes.Create();
        
        using Rfc2898DeriveBytes pdb = new(EncryptionKey, salt, 3, hashAlgorithm: HashAlgorithmName.SHA256);
        encryption.Key = pdb.GetBytes(secret_key_size);
        encryption.IV = pdb.GetBytes(iv_key_size);
        using MemoryStream ms = new();
        using CryptoStream cs = new(ms, encryption.CreateDecryptor(), CryptoStreamMode.Write);
        await cs.WriteAsync(cipherBytes);
        cs.Close();
        return ms.ToArray();
    }

    /// <summary>
    /// Расшифровать
    /// </summary>
    public static async Task<MemoryStream> DecryptAsync(MemoryStream ms, string EncryptionKey, byte[] salt) => new(await DecryptAsync(ms.ToArray(), EncryptionKey, salt));

    /// <summary>
    /// Энтропия
    /// </summary>
    public static byte[] GenerateRandomEntropy(int size)
    {
        byte[] randomBytes = new byte[size];
        using RandomNumberGenerator rngCsp = RandomNumberGenerator.Create();
        rngCsp.GetBytes(randomBytes);
        return randomBytes;
    }
}