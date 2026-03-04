Attribute VB_Name = "FischerIV"
Option Explicit

Private Const PI As Double = 3.14159265358979

Private Function NormPDF(x As Double) As Double
    NormPDF = Exp(-0.5 * x * x) / Sqr(2 * PI)
End Function

Private Function NormCDF(x As Double) As Double
    Dim t As Double, absX As Double
    Dim a1 As Double, a2 As Double, a3 As Double
    If x > 8 Then
        NormCDF = 1
        Exit Function
    ElseIf x < -8 Then
        NormCDF = 0
        Exit Function
    End If
    absX = Abs(x)
    t = 1 / (1 + 0.2316419 * absX)
    a1 = t * 1.330274429
    a2 = t * (-1.821255978 + a1)
    a3 = t * (1.781477937 + a2)
    a3 = t * (-0.356563782 + a3)
    a3 = t * (0.31938153 + a3)
    NormCDF = 1 - NormPDF(absX) * a3
    If x < 0 Then NormCDF = 1 - NormCDF
End Function

Private Sub CalcD1D2(S As Double, K As Double, T As Double, _
    r As Double, sigma As Double, q As Double, _
    ByRef d1 As Double, ByRef d2 As Double)
    Dim sqrtT As Double
    sqrtT = Sqr(T)
    d1 = (Log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
End Sub

Private Function MyMax(a As Double, b As Double) As Double
    If a > b Then MyMax = a Else MyMax = b
End Function

Public Function BSMPrice(S As Double, K As Double, T As Double, _
    r As Double, sigma As Double, q As Double, _
    ByVal OptType As String) As Double
    Dim d1 As Double, d2 As Double
    Dim disc As Double, fwdDisc As Double
    If T <= 0 Then
        If UCase(OptType) = "C" Then
            BSMPrice = MyMax(S - K, 0)
        Else
            BSMPrice = MyMax(K - S, 0)
        End If
        Exit Function
    End If
    If sigma <= 0 Then
        BSMPrice = 0
        Exit Function
    End If
    Call CalcD1D2(S, K, T, r, sigma, q, d1, d2)
    disc = Exp(-r * T)
    fwdDisc = Exp(-q * T)
    If UCase(OptType) = "C" Then
        BSMPrice = S * fwdDisc * NormCDF(d1) - K * disc * NormCDF(d2)
    Else
        BSMPrice = K * disc * NormCDF(-d2) - S * fwdDisc * NormCDF(-d1)
    End If
End Function

Private Function BSMVega(S As Double, K As Double, T As Double, _
    r As Double, sigma As Double, q As Double) As Double
    Dim d1 As Double, d2 As Double
    Call CalcD1D2(S, K, T, r, sigma, q, d1, d2)
    BSMVega = S * Exp(-q * T) * NormPDF(d1) * Sqr(T)
End Function

Private Function IVBisection(MktPrice As Double, S As Double, _
    K As Double, T As Double, r As Double, q As Double, _
    ByVal OptType As String, tol As Double) As Double
    Dim lo As Double, hi As Double, mid As Double
    Dim price As Double, finalPrice As Double
    Dim i As Long
    lo = 0.001
    hi = 10
    For i = 1 To 200
        mid = (lo + hi) / 2
        price = BSMPrice(S, K, T, r, mid, q, OptType)
        If Abs(price - MktPrice) < tol Then
            IVBisection = mid
            Exit Function
        End If
        If price > MktPrice Then
            hi = mid
        Else
            lo = mid
        End If
        If hi - lo < 0.00000001 Then Exit For
    Next i
    mid = (lo + hi) / 2
    finalPrice = BSMPrice(S, K, T, r, mid, q, OptType)
    If Abs(finalPrice - MktPrice) < tol * 10 Then
        IVBisection = mid
    Else
        IVBisection = -1
    End If
End Function

Public Function ImpliedVol(ByVal vS As Variant, ByVal vK As Variant, _
    ByVal vT As Variant, ByVal vR As Variant, _
    ByVal vQ As Variant, ByVal vMkt As Variant, _
    ByVal OptType As Variant) As Variant

    Dim S As Double, K As Double, T As Double
    Dim r As Double, q As Double, MktPrice As Double
    Dim sigma As Double, price As Double, diff As Double
    Dim vegaRaw As Double, intrinsic As Double
    Dim i As Long
    Const tol As Double = 0.001
    Const maxIter As Long = 100

    If Not IsNumeric(vS) Then GoTo BadInput
    If Not IsNumeric(vK) Then GoTo BadInput
    If Not IsNumeric(vT) Then GoTo BadInput
    If Not IsNumeric(vR) Then GoTo BadInput
    If Not IsNumeric(vQ) Then GoTo BadInput
    If Not IsNumeric(vMkt) Then GoTo BadInput
    If IsEmpty(vS) Or IsEmpty(vK) Then GoTo BadInput
    If IsEmpty(vT) Or IsEmpty(vMkt) Then GoTo BadInput

    S = CDbl(vS)
    K = CDbl(vK)
    T = CDbl(vT)
    r = CDbl(vR)
    q = CDbl(vQ)
    MktPrice = CDbl(vMkt)

    If T <= 0 Or MktPrice <= 0 Then GoTo BadInput
    If S <= 0 Or K <= 0 Then GoTo BadInput

    If UCase(OptType) = "C" Then
        intrinsic = MyMax(S - K, 0)
    Else
        intrinsic = MyMax(K - S, 0)
    End If
    If MktPrice < intrinsic - tol Then GoTo BadInput

    sigma = Sqr(2 * PI / T) * (MktPrice / S)
    If sigma < 0.01 Then sigma = 0.01
    If sigma > 5 Then sigma = 5

    For i = 1 To maxIter
        price = BSMPrice(S, K, T, r, sigma, q, CStr(OptType))
        diff = price - MktPrice
        If Abs(diff) < tol Then
            ImpliedVol = sigma
            Exit Function
        End If
        vegaRaw = BSMVega(S, K, T, r, sigma, q)
        If vegaRaw < 0.000000000001 Then
            sigma = IVBisection(MktPrice, S, K, T, r, q, CStr(OptType), tol)
            If sigma < 0 Then GoTo BadInput
            ImpliedVol = sigma
            Exit Function
        End If
        sigma = sigma - diff / vegaRaw
        If sigma < 0.001 Then sigma = 0.001
        If sigma > 10 Then sigma = 10
    Next i

    sigma = IVBisection(MktPrice, S, K, T, r, q, CStr(OptType), tol)
    If sigma < 0 Then GoTo BadInput
    ImpliedVol = sigma
    Exit Function

BadInput:
    ImpliedVol = ""
End Function
